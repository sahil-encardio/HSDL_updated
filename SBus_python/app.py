import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List
from contextlib import asynccontextmanager

from SBus import SBus, Message

# Definición de comandos
CMD_AN_EN = 0x01
CMD_AN_DI = 0x02
CMD_AN_POLL_EN = 0x03
CMD_AN_POLL_DI = 0x04

# Parámetros del sistema
MAX_MODULE = 10
MAX_CHANNEL = 16

# Crear un canal de comunicación y configurar el publicador
publisher_sbus = SBus("sam_channel_data")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    publisher_sbus.init_publisher()
    yield
    # Shutdown logic
    publisher_sbus.destroy()

# Inicializar FastAPI con el gestor de ciclo de vida
app = FastAPI(lifespan=lifespan)

class ChannelRequest(BaseModel):
    """
    Input schema for enabling or disabling channels on a module.

    This class defines the input fields required to enable or disable channels for a given module
    along with their descriptions and validation constraints.

    :param module_id: The module ID (integer), must be within the allowed range (0 to MAX_MODULE-1).
    :param channels: List of channel IDs (list of integers).
    """
    module_id: int = Field(..., ge=0, le=MAX_MODULE-1, description="Module ID within allowed range.")
    channels: List[int] = Field(..., description="List of channel IDs to be enabled or disabled.")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "module_id": 3,
                    "channels": [1, 2, 5],
                }
            ]
        }
    }

class PollRequest(BaseModel):
    """
    Input schema for enabling or disabling polling on a module.

    This class defines the input fields required to enable or disable polling for a given module
    along with their descriptions and validation constraints.

    :param module_id: The module ID (integer), must be within the allowed range (0 to MAX_MODULE-1).
    """
    module_id: int = Field(..., ge=0, le=MAX_MODULE-1, description="Module ID within allowed range.")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "module_id": 4,
                }
            ]
        }
    }

def send_enable_channels(module_id: int, channels: List[int]) -> dict:
    """
    Send command to enable specified channels for a given module.

    :param module_id: The ID of the module to configure.
    :param channels: List of channel IDs to be enabled.
    :return: A dictionary with the result message.
    """
    if module_id >= MAX_MODULE:
        raise HTTPException(status_code=400, detail=f"Module ID {module_id} out of range (max: {MAX_MODULE - 1})")

    data = bytes([CMD_AN_EN, module_id, len(channels)] + channels)
    message = Message(data)
    publisher_sbus.send_message(message)
    return {"message": f"Sent CMD_AN_EN: mod_id={module_id}, channels={channels}"}

def send_disable_channels(module_id: int, channels: List[int]) -> dict:
    """
    Send command to disable specified channels for a given module.

    :param module_id: The ID of the module to configure.
    :param channels: List of channel IDs to be disabled.
    :return: A dictionary with the result message.
    """
    if module_id >= MAX_MODULE:
        raise HTTPException(status_code=400, detail=f"Module ID {module_id} out of range (max: {MAX_MODULE - 1})")

    data = bytes([CMD_AN_DI, module_id, len(channels)] + channels)
    message = Message(data)
    publisher_sbus.send_message(message)
    return {"message": f"Sent CMD_AN_DI: mod_id={module_id}, channels={channels}"}

def send_enable_poll(module_id: int) -> dict:
    """
    Send command to enable polling for a given module.

    :param module_id: The ID of the module to configure.
    :return: A dictionary with the result message.
    """
    if module_id >= MAX_MODULE:
        raise HTTPException(status_code=400, detail=f"Module ID {module_id} out of range (max: {MAX_MODULE - 1})")

    data = bytes([CMD_AN_POLL_EN, module_id])
    message = Message(data)
    publisher_sbus.send_message(message)
    return {"message": f"Sent CMD_AN_POLL_EN: mod_id={module_id}"}

def send_disable_poll(module_id: int) -> dict:
    """
    Send command to disable polling for a given module.

    :param module_id: The ID of the module to configure.
    :return: A dictionary with the result message.
    """
    if module_id >= MAX_MODULE:
        raise HTTPException(status_code=400, detail=f"Module ID {module_id} out of range (max: {MAX_MODULE - 1})")

    data = bytes([CMD_AN_POLL_DI, module_id])
    message = Message(data)
    publisher_sbus.send_message(message)
    return {"message": f"Sent CMD_AN_POLL_DI: mod_id={module_id}"}

@app.post("/enable_channels")
def api_enable_channels(request: ChannelRequest):
    """
    API endpoint for enabling specified channels on a module.

    :param request: The request body containing module ID and channels to enable.
    :return: A JSON response indicating the result.
    """
    return send_enable_channels(request.module_id, request.channels)

@app.post("/disable_channels")
def api_disable_channels(request: ChannelRequest):
    """
    API endpoint for disabling specified channels on a module.

    :param request: The request body containing module ID and channels to disable.
    :return: A JSON response indicating the result.
    """
    return send_disable_channels(request.module_id, request.channels)

@app.post("/enable_poll")
def api_enable_poll(request: PollRequest):
    """
    API endpoint for enabling polling on a module.

    :param request: The request body containing module ID.
    :return: A JSON response indicating the result.
    """
    return send_enable_poll(request.module_id)

@app.post("/disable_poll")
def api_disable_poll(request: PollRequest):
    """
    API endpoint for disabling polling on a module.

    :param request: The request body containing module ID.
    :return: A JSON response indicating the result.
    """
    return send_disable_poll(request.module_id)

