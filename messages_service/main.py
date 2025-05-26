from fastapi import FastAPI
import uvicorn
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)

logger = logging.getLogger(__name__)
logger.info("Messages-Service starting up...")

app = FastAPI()

@app.get("/message/")
def get_message():
    logger.info("Handled GET request at /message/")
    return {"message": "Messages Service Not Implemented Yet!"}

if __name__ == "__main__":
    logger.info("Messages-Service running on port 8002")
    uvicorn.run(app, host="0.0.0.0", port=8002)
