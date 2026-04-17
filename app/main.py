from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from app.database import init_db, SessionLocal
from app.snowflake_crawler import polling_worker as snowflake_polling_worker
from app.services.dbt_crawler import polling_worker as dbt_polling_worker
from app.utils.models import SnowflakeConnection, SnowflakeJob
from app.utils.websocket_manager import websocket_manager
import threading
import asyncio
from app.api import auth, organizations, snowflake, github, jira, impact, dbt_cloud, chat, users, overview_dashboard
from app.data_catalog import router as data_catalog_router
from scripts import init_product_support_admin
import logging
import sys
from app.vector_db import upsert_lineage_embeddings
from sqlalchemy import or_
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("app")


def sync_jobs_with_connections():
    """Ensure all connections with cron expressions have corresponding job entries"""
    db = SessionLocal()
    try:
        # Find all active connections with cron expressions
        connections_with_cron = db.query(SnowflakeConnection).filter(
            SnowflakeConnection.is_active == True,
            SnowflakeConnection.cron_expression.isnot(None),
            SnowflakeConnection.cron_expression != ""
        ).all()
        
        logger.info("🔍 Found %d connections with cron expressions", len(connections_with_cron))
        
        jobs_created = 0
        jobs_updated = 0
        jobs_deactivated = 0
        
        for connection in connections_with_cron:
            # Check if job already exists for this connection
            existing_job = db.query(SnowflakeJob).filter(
                SnowflakeJob.connection_id == connection.id
            ).first()
            
            if existing_job:
                # Update existing job if cron expression changed
                if existing_job.cron_expression != connection.cron_expression:
                    existing_job.cron_expression = connection.cron_expression
                    existing_job.is_active = True
                    jobs_updated += 1
                    logger.info("🔄 Updated job for connection: %s", connection.connection_name)
            else:
                # Create new job entry
                new_job = SnowflakeJob(
                    connection_id=connection.id,
                    cron_expression=connection.cron_expression,
                    last_run_time=None,
                    is_active=True
                )
                db.add(new_job)
                jobs_created += 1
                logger.info("➕ Created job for connection: %s", connection.connection_name)
        
        # Handle connections with empty cron expressions - deactivate their jobs
        connections_without_cron = db.query(SnowflakeConnection).filter(
            SnowflakeConnection.is_active == True,
            or_(
                SnowflakeConnection.cron_expression.is_(None),
                SnowflakeConnection.cron_expression == ""
            )
        ).all()
        
        for connection in connections_without_cron:
            existing_job = db.query(SnowflakeJob).filter(
                SnowflakeJob.connection_id == connection.id,
                SnowflakeJob.is_active == True
            ).first()
            
            if existing_job:
                existing_job.is_active = False
                jobs_deactivated += 1
                logger.info("⏸️  Deactivated job for connection (no cron): %s", connection.connection_name)
        
        # Commit all changes
        db.commit()
        
        if jobs_created > 0 or jobs_updated > 0 or jobs_deactivated > 0:
            logger.info("✅ Job sync completed: %d created, %d updated, %d deactivated", 
                       jobs_created, jobs_updated, jobs_deactivated)
        else:
            logger.info("✅ Job sync completed: no changes needed")
            
    except Exception as e:
        logger.exception("❌ Error syncing jobs with connections: %s", str(e))
        db.rollback()
    finally:
        db.close()


app = FastAPI(
    title="QueryGuardAI Backend",
    description="Backend API for QueryGuardAI - Data Lineage and Impact Analysis Tool",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router)
app.include_router(organizations.router)
app.include_router(users.router)
app.include_router(snowflake.router)
app.include_router(github.router)
app.include_router(jira.router)
app.include_router(impact.router)
app.include_router(dbt_cloud.router)
app.include_router(chat.router)
app.include_router(overview_dashboard.router)
app.include_router(data_catalog_router)
# TEMPORARY: Initialization endpoint - remove after setup
app.include_router(init_product_support_admin.router)

@app.on_event("startup")
async def startup_event():
    """Initialize database and start background worker on startup"""
    logger.info("🚀 Application startup initiated")
    
    # Initialize database
    init_db()
    logger.info("📊 Database initialized")
    ## Initialize vector database for intelytics org from column_level_lineage table
    # upsert_lineage_embeddings(org_id="76d33fb3-6062-456b-a211-4aec9971f8be")
    # logger.info("Vector database initialized from column_level_lineage for intelytics org")
    
    # Sync jobs with connections
    sync_jobs_with_connections()
    
    # Start background polling worker
    app.state.worker_stop_event = threading.Event()
    app.state.worker_thread = threading.Thread(
        target=snowflake_polling_worker, 
        args=(app.state.worker_stop_event,), 
        daemon=True,
        name="SnowflakeCrawlerWorker"
    )
    app.state.worker_thread.start()
    # Start dbt crawler worker
    app.state.dbt_worker_stop_event = threading.Event()
    app.state.dbt_worker_thread = threading.Thread(
        target=dbt_polling_worker,
        args=(app.state.dbt_worker_stop_event, 30),
        daemon=True,
        name="DbtCrawlerWorker"
    )
    app.state.dbt_worker_thread.start()
    
    # Start WebSocket cleanup task
    app.state.websocket_cleanup_task = asyncio.create_task(websocket_cleanup_worker())
    
    # Verify worker started
    if app.state.worker_thread.is_alive():
        logger.info("🔄 Snowflake crawler worker started")
    else:
        logger.error("❌ Failed to start Snowflake crawler worker")
    if app.state.dbt_worker_thread.is_alive():
        logger.info("🔄 dbt crawler worker started")
    else:
        logger.error("❌ Failed to start dbt crawler worker")
    
    logger.info("🔗 WebSocket manager initialized")
    logger.info("✅ Application startup completed")

async def websocket_cleanup_worker():
    """Background task to cleanup inactive WebSocket sessions"""
    while True:
        try:
            await asyncio.sleep(300)  # Run every 5 minutes
            await websocket_manager.cleanup_inactive_sessions(timeout_minutes=30)
        except asyncio.CancelledError:
            logger.info("WebSocket cleanup worker cancelled")
            break
        except Exception as e:
            logger.error(f"Error in WebSocket cleanup worker: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop background worker on shutdown"""
    logger.info("🛑 Application shutdown initiated")
    
    # Cancel WebSocket cleanup task
    if hasattr(app.state, 'websocket_cleanup_task'):
        app.state.websocket_cleanup_task.cancel()
        try:
            await app.state.websocket_cleanup_task
        except asyncio.CancelledError:
            pass
        logger.info("🔗 WebSocket cleanup worker stopped")
    
    if hasattr(app.state, 'worker_stop_event'):
        app.state.worker_stop_event.set()
        
        if hasattr(app.state, 'worker_thread') and app.state.worker_thread.is_alive():
            app.state.worker_thread.join(timeout=10)  # Wait up to 10 seconds
            
            if app.state.worker_thread.is_alive():
                logger.warning("⚠️  Worker thread did not stop gracefully")
            else:
                logger.info("🔄 Snowflake crawler worker stopped")
    if hasattr(app.state, 'dbt_worker_stop_event'):
        app.state.dbt_worker_stop_event.set()
        if hasattr(app.state, 'dbt_worker_thread') and app.state.dbt_worker_thread.is_alive():
            app.state.dbt_worker_thread.join(timeout=10)
            if app.state.dbt_worker_thread.is_alive():
                logger.warning("⚠️  dbt worker thread did not stop gracefully")
            else:
                logger.info("🔄 dbt crawler worker stopped")
    
    logger.info("✅ Application shutdown completed")

@app.get("/")
async def root(request: Request):
    logger.info("GET / - Root endpoint called from %s", request.client.host if request.client else "unknown")
    return {"message": "QueryGuardAI Backend API", "version": "1.0.0"}

@app.get("/health")
async def health_check(request: Request):
    logger.debug("GET /health - Health check from %s", request.client.host if request.client else "unknown")
    return {"status": "healthy"}

@app.get("/worker-status")
async def worker_status(request: Request):
    """Check the status of the background Snowflake crawler worker"""
    logger.info("GET /worker-status - Worker status check from %s", request.client.host if request.client else "unknown")
    
    status = {
        "worker_running": False,
        "thread_alive": False,
        "thread_name": None,
        "thread_id": None,
        "stop_event_set": False,
        "websocket_stats": {}
    }
    
    if hasattr(app.state, 'worker_thread'):
        status["thread_alive"] = app.state.worker_thread.is_alive()
        status["thread_name"] = app.state.worker_thread.name
        status["thread_id"] = app.state.worker_thread.ident
        status["worker_running"] = status["thread_alive"]
    
    if hasattr(app.state, 'worker_stop_event'):
        status["stop_event_set"] = app.state.worker_stop_event.is_set()
    
    # Add WebSocket statistics
    try:
        status["websocket_stats"] = websocket_manager.get_stats()
    except Exception as e:
        status["websocket_stats"] = {"error": str(e)}
    
    logger.info("Worker status: %s", status)
    return status

