#!/usr/bin/env python3
"""
Meta-router for llama.cpp Kubernetes cluster.

Routes requests to warm nodes with loaded models or triggers model loading
using llama router mode. Designed for efficient use of GPU resources on
Strix Halo cluster nodes.
"""

import asyncio
import logging
import os
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

import aiohttp
from aiohttp import web
from kubernetes import client, config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class NodeState:
    """Track the state of a llama.cpp node."""
    hostname: str
    endpoint: str
    model: Optional[str] = None
    last_health_check: Optional[datetime] = None
    is_healthy: bool = False
    is_warm: bool = False


class MetaRouter:
    """
    Meta-router for managing llama.cpp pods across Kubernetes nodes.
    
    Handles request routing to warm nodes and model loading on demand.
    """
    
    def __init__(self):
        self.nodes: Dict[str, NodeState] = {}
        self.model_to_nodes: Dict[str, List[str]] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self.health_check_interval = int(os.getenv('HEALTH_CHECK_INTERVAL', '30'))
        self.llama_port = int(os.getenv('LLAMA_PORT', '8080'))
        self.namespace = os.getenv('NAMESPACE', 'default')
        
        # Initialize Kubernetes client
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        
        self.k8s_v1 = client.CoreV1Api()
    
    async def initialize(self):
        """Initialize the router and discover nodes."""
        self.session = aiohttp.ClientSession()
        await self.discover_nodes()
        asyncio.create_task(self.health_check_loop())
    
    async def discover_nodes(self):
        """Discover llama.cpp pods running in the cluster."""
        logger.info("Discovering llama.cpp nodes...")
        
        try:
            # Find all pods with llama.cpp label
            pods = self.k8s_v1.list_namespaced_pod(
                self.namespace,
                label_selector="app=llamacpp"
            )
            
            for pod in pods.items:
                if pod.status.pod_ip and pod.status.phase == "Running":
                    hostname = pod.metadata.name
                    endpoint = f"http://{pod.status.pod_ip}:{self.llama_port}"
                    
                    self.nodes[hostname] = NodeState(
                        hostname=hostname,
                        endpoint=endpoint
                    )
                    logger.info(f"Discovered node: {hostname} at {endpoint}")
        
        except Exception as e:
            logger.error(f"Error discovering nodes: {e}")
    
    async def check_node_health(self, node: NodeState) -> bool:
        """Check if a node is healthy and what model it has loaded."""
        try:
            async with self.session.get(
                f"{node.endpoint}/health",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    node.is_healthy = True
                    node.last_health_check = datetime.now()
                    
                    # Check if model is loaded
                    if data.get('model'):
                        node.model = data['model']
                        node.is_warm = True
                        
                        # Update model mapping
                        if node.model not in self.model_to_nodes:
                            self.model_to_nodes[node.model] = []
                        if node.hostname not in self.model_to_nodes[node.model]:
                            self.model_to_nodes[node.model].append(node.hostname)
                    else:
                        node.is_warm = False
                    
                    return True
        except Exception as e:
            logger.debug(f"Health check failed for {node.hostname}: {e}")
        
        node.is_healthy = False
        node.is_warm = False
        return False
    
    async def health_check_loop(self):
        """Periodically check health of all nodes."""
        while True:
            await asyncio.sleep(self.health_check_interval)
            logger.debug("Running health checks...")
            
            tasks = [
                self.check_node_health(node)
                for node in self.nodes.values()
            ]
            
            if tasks:
                await asyncio.gather(*tasks)
    
    async def get_warm_node(self, model: str) -> Optional[NodeState]:
        """Get a warm node that has the requested model loaded."""
        if model in self.model_to_nodes:
            for hostname in self.model_to_nodes[model]:
                node = self.nodes.get(hostname)
                if node and node.is_healthy and node.is_warm:
                    return node
        return None
    
    async def get_available_node(self) -> Optional[NodeState]:
        """Get any healthy node that can load a model."""
        for node in self.nodes.values():
            if node.is_healthy and not node.is_warm:
                return node
        
        # If all nodes are warm, return any healthy node
        for node in self.nodes.values():
            if node.is_healthy:
                return node
        
        return None
    
    async def load_model(self, node: NodeState, model: str) -> bool:
        """Load a model on a specific node."""
        logger.info(f"Loading model {model} on {node.hostname}")
        
        try:
            async with self.session.post(
                f"{node.endpoint}/load",
                json={"model": model},
                timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                if resp.status == 200:
                    node.model = model
                    node.is_warm = True
                    
                    if model not in self.model_to_nodes:
                        self.model_to_nodes[model] = []
                    if node.hostname not in self.model_to_nodes[model]:
                        self.model_to_nodes[model].append(node.hostname)
                    
                    logger.info(f"Successfully loaded {model} on {node.hostname}")
                    return True
        except Exception as e:
            logger.error(f"Error loading model on {node.hostname}: {e}")
        
        return False
    
    async def route_request(self, request: web.Request) -> web.Response:
        """Route a request to an appropriate node."""
        try:
            # Extract model from request
            data = await request.json()
            model = data.get('model', 'default')
            
            # Try to find a warm node with the model
            node = await self.get_warm_node(model)
            
            if not node:
                # No warm node, try to load model on available node
                logger.info(f"No warm node for model {model}, attempting to load")
                node = await self.get_available_node()
                
                if node:
                    success = await self.load_model(node, model)
                    if not success:
                        return web.json_response(
                            {"error": "Failed to load model"},
                            status=503
                        )
                else:
                    return web.json_response(
                        {"error": "No available nodes"},
                        status=503
                    )
            
            # Forward request to the node
            logger.info(f"Routing request to {node.hostname} for model {model}")
            
            async with self.session.post(
                f"{node.endpoint}/v1/completions",
                json=data,
                headers=request.headers
            ) as resp:
                response_data = await resp.json()
                return web.json_response(response_data, status=resp.status)
        
        except Exception as e:
            logger.error(f"Error routing request: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
    
    async def handle_health(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        healthy_nodes = sum(1 for n in self.nodes.values() if n.is_healthy)
        warm_nodes = sum(1 for n in self.nodes.values() if n.is_warm)
        
        return web.json_response({
            "status": "healthy",
            "total_nodes": len(self.nodes),
            "healthy_nodes": healthy_nodes,
            "warm_nodes": warm_nodes,
            "models": list(self.model_to_nodes.keys())
        })
    
    async def handle_status(self, request: web.Request) -> web.Response:
        """Status endpoint with detailed node information."""
        nodes_status = [
            {
                "hostname": node.hostname,
                "endpoint": node.endpoint,
                "is_healthy": node.is_healthy,
                "is_warm": node.is_warm,
                "model": node.model,
                "last_health_check": node.last_health_check.isoformat() if node.last_health_check else None
            }
            for node in self.nodes.values()
        ]
        
        return web.json_response({
            "nodes": nodes_status,
            "model_to_nodes": self.model_to_nodes
        })
    
    async def cleanup(self):
        """Cleanup resources."""
        if self.session:
            await self.session.close()


async def init_app():
    """Initialize the web application."""
    app = web.Application()
    router = MetaRouter()
    
    await router.initialize()
    
    # Setup routes
    app.router.add_post('/v1/completions', router.route_request)
    app.router.add_post('/v1/chat/completions', router.route_request)
    app.router.add_get('/health', router.handle_health)
    app.router.add_get('/status', router.handle_status)
    
    # Cleanup on shutdown
    async def cleanup_app(app):
        await router.cleanup()
    
    app.on_cleanup.append(cleanup_app)
    
    return app


def main():
    """Main entry point."""
    port = int(os.getenv('PORT', '8000'))
    web.run_app(init_app(), port=port)


if __name__ == '__main__':
    main()
