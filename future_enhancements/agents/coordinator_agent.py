"""
Coordinator Agent for MerchantIQ system.
Orchestrates all agents and manages the complete data processing pipeline.
"""

import asyncio
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import yaml

from .data_ingestion_agent import DataIngestionAgent
from .feature_engineering_agent import FeatureEngineeringAgent
from .attribution_agent import AttributionAgent
from .merchant_scoring_agent import MerchantScoringAgent
from .insight_agent import InsightAgent

from ..utils.logger import get_logger, performance_log, AgentActionContext


class PipelineStage(Enum):
    """Pipeline execution stages."""
    INITIALIZATION = "initialization"
    DATA_INGESTION = "data_ingestion"
    FEATURE_ENGINEERING = "feature_engineering"
    ATTRIBUTION = "attribution"
    SCORING = "scoring"
    INSIGHTS = "insights"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PipelineResult:
    """Result of a complete pipeline execution."""
    execution_id: str
    started_at: datetime
    completed_at: Optional[datetime]
    stage: PipelineStage
    
    # Data results
    unified_events_count: int
    merchants_processed: int
    features_computed: int
    attributions_computed: int
    scores_computed: int
    insights_generated: int
    
    # Performance metrics
    total_execution_time_ms: float
    stage_timings: Dict[str, float]
    
    # Agent status
    agent_statuses: Dict[str, Dict[str, Any]]
    
    # Errors
    errors: List[str]
    warnings: List[str]
    
    # Success flag
    success: bool


@dataclass
class SystemStatus:
    """Overall system status."""
    is_running: bool
    last_execution: Optional[datetime]
    total_executions: int
    successful_executions: int
    failed_executions: int
    
    # Current pipeline
    current_execution_id: Optional[str]
    current_stage: Optional[PipelineStage]
    
    # Agent health
    agents_healthy: Dict[str, bool]
    
    # Configuration
    config_loaded: bool
    environment: str


class CoordinatorAgent:
    """Central coordinator that orchestrates the entire MerchantIQ pipeline."""
    
    def __init__(self, config_path: str = "config/lenses_mcp.yaml"):
        """Initialize the Coordinator Agent.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.logger = get_logger("agent.coordinator")
        self.config = self._load_config()
        
        # Agent state
        self.is_running = False
        self.execution_id_counter = 0
        self.pipeline_results: Dict[str, PipelineResult] = {}
        
        # Initialize agents
        self.agents = self._initialize_agents()
        
        # Pipeline configuration
        pipeline_config = self.config.get('pipeline', {})
        self.auto_retry = pipeline_config.get('auto_retry', True)
        self.max_retries = pipeline_config.get('max_retries', 3)
        self.retry_delay_seconds = pipeline_config.get('retry_delay_seconds', 60)
        self.parallel_execution = pipeline_config.get('parallel_execution', False)
        
        # Monitoring
        self.health_check_interval = pipeline_config.get('health_check_interval_minutes', 5)
        self.last_health_check: Optional[datetime] = None
        
        # Data persistence
        self.persist_results = pipeline_config.get('persist_results', True)
        self.results_cache_size = pipeline_config.get('results_cache_size', 10)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except Exception as e:
            self.logger.error(f"Failed to load config from {self.config_path}: {e}")
            return {}
    
    def _initialize_agents(self) -> Dict[str, Any]:
        """Initialize all agents with configuration."""
        agents = {}
        
        try:
            # Initialize each agent with relevant config section
            agents['data_ingestion'] = DataIngestionAgent(
                environment=self.config.get('lenses', {}).get('environment', 'financial-data'),
                config=self.config.get('data_ingestion', {})
            )
            
            agents['feature_engineering'] = FeatureEngineeringAgent(
                config=self.config.get('feature_engineering', {})
            )
            
            agents['attribution'] = AttributionAgent(
                config=self.config.get('attribution', {})
            )
            
            agents['merchant_scoring'] = MerchantScoringAgent(
                config=self.config.get('ml', {})
            )
            
            agents['insight'] = InsightAgent(
                config=self.config.get('ai', {})
            )
            
            self.logger.info("All agents initialized successfully")
            return agents
            
        except Exception as e:
            self.logger.error(f"Agent initialization failed: {e}")
            return {}
    
    @performance_log
    async def start(self) -> None:
        """Start the coordinator and all agents."""
        with AgentActionContext("coordinator", "start"):
            try:
                self.is_running = True
                
                # Start all agents
                for agent_name, agent in self.agents.items():
                    await agent.start()
                    self.logger.info(f"Started {agent_name} agent")
                
                self.logger.info("Coordinator Agent started - all agents active")
                
            except Exception as e:
                self.logger.error(f"Failed to start coordinator: {e}")
                await self.stop()
                raise
    
    async def stop(self) -> None:
        """Stop the coordinator and all agents."""
        with AgentActionContext("coordinator", "stop"):
            self.is_running = False
            
            # Stop all agents
            for agent_name, agent in self.agents.items():
                try:
                    await agent.stop()
                    self.logger.info(f"Stopped {agent_name} agent")
                except Exception as e:
                    self.logger.error(f"Error stopping {agent_name} agent: {e}")
            
            self.logger.info("Coordinator Agent stopped")
    
    @performance_log
    async def execute_pipeline(self, force_refresh: bool = False) -> PipelineResult:
        """Execute the complete MerchantIQ pipeline.
        
        Args:
            force_refresh: Force refresh of all data regardless of cache
            
        Returns:
            PipelineResult with execution details and results
        """
        execution_id = f"exec_{self.execution_id_counter:04d}_{int(datetime.now().timestamp())}"
        self.execution_id_counter += 1
        
        start_time = datetime.now()
        stage_timings = {}
        errors = []
        warnings = []
        
        result = PipelineResult(
            execution_id=execution_id,
            started_at=start_time,
            completed_at=None,
            stage=PipelineStage.INITIALIZATION,
            unified_events_count=0,
            merchants_processed=0,
            features_computed=0,
            attributions_computed=0,
            scores_computed=0,
            insights_generated=0,
            total_execution_time_ms=0,
            stage_timings=stage_timings,
            agent_statuses={},
            errors=errors,
            warnings=warnings,
            success=False
        )
        
        with AgentActionContext("coordinator", "execute_pipeline", execution_id):
            try:
                # Stage 1: Data Ingestion
                result.stage = PipelineStage.DATA_INGESTION
                stage_start = datetime.now()
                self.logger.info(f"[{execution_id}] Starting data ingestion")
                
                unified_events = await self.agents['data_ingestion'].ingest_data()
                result.unified_events_count = len(unified_events)
                stage_timings['data_ingestion'] = (datetime.now() - stage_start).total_seconds() * 1000
                
                if not unified_events:
                    raise Exception("No data ingested - cannot proceed with pipeline")
                
                self.logger.info(f"[{execution_id}] Data ingestion completed: {len(unified_events)} events")
                
                # Stage 2: Feature Engineering
                result.stage = PipelineStage.FEATURE_ENGINEERING
                stage_start = datetime.now()
                self.logger.info(f"[{execution_id}] Starting feature engineering")
                
                merchant_features = await self.agents['feature_engineering'].compute_features(unified_events)
                result.features_computed = len(merchant_features)
                stage_timings['feature_engineering'] = (datetime.now() - stage_start).total_seconds() * 1000
                
                if not merchant_features:
                    raise Exception("No merchant features computed - cannot proceed")
                
                self.logger.info(f"[{execution_id}] Feature engineering completed: {len(merchant_features)} merchants")
                
                # Stage 3: Attribution Analysis
                result.stage = PipelineStage.ATTRIBUTION
                stage_start = datetime.now()
                self.logger.info(f"[{execution_id}] Starting attribution analysis")
                
                attribution_results = await self.agents['attribution'].compute_attribution(unified_events)
                result.attributions_computed = len(attribution_results)
                stage_timings['attribution'] = (datetime.now() - stage_start).total_seconds() * 1000
                
                self.logger.info(f"[{execution_id}] Attribution analysis completed: {len(attribution_results)} merchants")
                
                # Stage 4: Merchant Scoring
                result.stage = PipelineStage.SCORING
                stage_start = datetime.now()
                self.logger.info(f"[{execution_id}] Starting merchant scoring")
                
                scoring_results = await self.agents['merchant_scoring'].score_merchants(
                    merchant_features, attribution_results
                )
                result.scores_computed = len(scoring_results)
                stage_timings['scoring'] = (datetime.now() - stage_start).total_seconds() * 1000
                
                if not scoring_results:
                    warnings.append("No merchant scores computed")
                
                self.logger.info(f"[{execution_id}] Merchant scoring completed: {len(scoring_results)} merchants")
                
                # Stage 5: Insight Generation
                result.stage = PipelineStage.INSIGHTS
                stage_start = datetime.now()
                self.logger.info(f"[{execution_id}] Starting insight generation")
                
                business_insights = await self.agents['insight'].generate_insights(
                    merchant_features, attribution_results, scoring_results
                )
                
                # Count total insights generated
                total_insights = sum(len(insights) for insights in business_insights.values())
                result.insights_generated = total_insights
                stage_timings['insights'] = (datetime.now() - stage_start).total_seconds() * 1000
                
                self.logger.info(f"[{execution_id}] Insight generation completed: {total_insights} insights")
                
                # Pipeline completed successfully
                result.stage = PipelineStage.COMPLETED
                result.success = True
                result.merchants_processed = len(merchant_features)
                
                # Collect agent statuses
                result.agent_statuses = await self._collect_agent_statuses()
                
                self.logger.info(f"[{execution_id}] Pipeline execution completed successfully")
                
            except Exception as e:
                result.stage = PipelineStage.FAILED
                result.success = False
                errors.append(str(e))
                
                self.logger.error(f"[{execution_id}] Pipeline execution failed: {e}")
                
                # Still collect agent statuses for debugging
                try:
                    result.agent_statuses = await self._collect_agent_statuses()
                except:
                    pass
            
            # Finalize result
            result.completed_at = datetime.now()
            result.total_execution_time_ms = (result.completed_at - start_time).total_seconds() * 1000
            
            # Cache result
            self.pipeline_results[execution_id] = result
            
            # Clean up old results if cache is full
            if len(self.pipeline_results) > self.results_cache_size:
                oldest_key = min(self.pipeline_results.keys())
                del self.pipeline_results[oldest_key]
            
            return result
    
    async def _collect_agent_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Collect status from all agents."""
        statuses = {}
        
        for agent_name, agent in self.agents.items():
            try:
                status = agent.get_status()
                statuses[agent_name] = status
            except Exception as e:
                statuses[agent_name] = {"error": str(e), "healthy": False}
        
        return statuses
    
    @performance_log
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all agents and system."""
        with AgentActionContext("coordinator", "health_check"):
            health_status = {
                "coordinator": {
                    "healthy": self.is_running,
                    "last_check": datetime.now().isoformat(),
                    "pipeline_results_cached": len(self.pipeline_results)
                }
            }
            
            # Check each agent
            for agent_name, agent in self.agents.items():
                try:
                    agent_status = agent.get_status()
                    health_status[agent_name] = {
                        "healthy": agent_status.get("is_running", False),
                        "status": agent_status
                    }
                except Exception as e:
                    health_status[agent_name] = {
                        "healthy": False,
                        "error": str(e)
                    }
            
            self.last_health_check = datetime.now()
            return health_status
    
    async def get_system_status(self) -> SystemStatus:
        """Get comprehensive system status."""
        # Count executions
        total_executions = len(self.pipeline_results)
        successful_executions = sum(1 for r in self.pipeline_results.values() if r.success)
        failed_executions = total_executions - successful_executions
        
        # Get last execution
        last_execution = None
        if self.pipeline_results:
            latest_result = max(self.pipeline_results.values(), key=lambda x: x.started_at)
            last_execution = latest_result.started_at
        
        # Check agent health
        agents_healthy = {}
        for agent_name, agent in self.agents.items():
            try:
                status = agent.get_status()
                agents_healthy[agent_name] = status.get("is_running", False)
            except:
                agents_healthy[agent_name] = False
        
        return SystemStatus(
            is_running=self.is_running,
            last_execution=last_execution,
            total_executions=total_executions,
            successful_executions=successful_executions,
            failed_executions=failed_executions,
            current_execution_id=None,  # Would track active execution
            current_stage=None,  # Would track current stage
            agents_healthy=agents_healthy,
            config_loaded=bool(self.config),
            environment=self.config.get('lenses', {}).get('environment', 'unknown')
        )
    
    async def get_pipeline_result(self, execution_id: str) -> Optional[PipelineResult]:
        """Get specific pipeline execution result."""
        return self.pipeline_results.get(execution_id)
    
    async def get_latest_pipeline_result(self) -> Optional[PipelineResult]:
        """Get the most recent pipeline execution result."""
        if not self.pipeline_results:
            return None
        
        return max(self.pipeline_results.values(), key=lambda x: x.started_at)
    
    async def get_pipeline_history(self, limit: int = 10) -> List[PipelineResult]:
        """Get pipeline execution history."""
        results = sorted(self.pipeline_results.values(), key=lambda x: x.started_at, reverse=True)
        return results[:limit]
    
    async def retry_failed_pipeline(self, execution_id: str) -> Optional[PipelineResult]:
        """Retry a failed pipeline execution."""
        original_result = self.pipeline_results.get(execution_id)
        
        if not original_result or original_result.success:
            self.logger.warning(f"Cannot retry execution {execution_id}: not found or already successful")
            return None
        
        self.logger.info(f"Retrying failed pipeline execution: {execution_id}")
        return await self.execute_pipeline(force_refresh=True)
    
    async def cleanup_old_results(self, days: int = 7) -> int:
        """Clean up old pipeline results."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        old_results = [
            exec_id for exec_id, result in self.pipeline_results.items()
            if result.started_at < cutoff_date
        ]
        
        for exec_id in old_results:
            del self.pipeline_results[exec_id]
        
        self.logger.info(f"Cleaned up {len(old_results)} old pipeline results")
        return len(old_results)
    
    def get_status(self) -> Dict[str, Any]:
        """Get coordinator status."""
        return {
            "agent_name": "CoordinatorAgent",
            "is_running": self.is_running,
            "last_health_check": self.last_health_check.isoformat() if self.last_health_check else None,
            "pipeline_results_count": len(self.pipeline_results),
            "agents_initialized": len(self.agents),
            "config_loaded": bool(self.config),
            "configuration": {
                "auto_retry": self.auto_retry,
                "max_retries": self.max_retries,
                "parallel_execution": self.parallel_execution,
                "results_cache_size": self.results_cache_size
            }
        }
    
    # Convenience methods for accessing agent results
    async def get_latest_merchant_features(self) -> Dict[str, Any]:
        """Get latest merchant features from feature engineering agent."""
        if 'feature_engineering' in self.agents:
            return self.agents['feature_engineering'].feature_cache
        return {}
    
    async def get_latest_scores(self) -> Dict[str, Any]:
        """Get latest merchant scores from scoring agent."""
        if 'merchant_scoring' in self.agents:
            return self.agents['merchant_scoring'].score_cache
        return {}
    
    async def get_latest_insights(self) -> Dict[str, Any]:
        """Get latest business insights from insight agent."""
        if 'insight' in self.agents:
            return self.agents['insight'].insight_cache
        return {}
    
    async def get_top_merchants(self, metric: str = "composite_score", limit: int = 10) -> List[Tuple[str, float]]:
        """Get top merchants by specified metric."""
        if 'merchant_scoring' in self.agents:
            return await self.agents['merchant_scoring'].get_top_merchants(metric, limit)
        return []