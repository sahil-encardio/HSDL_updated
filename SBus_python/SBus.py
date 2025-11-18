"""
# SBus Python Library

@copyright Copyright (c) 2025

This software is the confidential and proprietary information of ENCARDIO.  
You shall not disclose such Confidential Information and shall  
use it only in accordance with the terms of the license agreement  
you entered into with ENCARDIO.  
"""
import zmq
import os
import threading
import time

ENDPOINT_FORMAT = "ipc:///tmp/feeds/{}"


class Message:
    """ 
    Represents a message in the messaging system. 
    """

    def __init__(self, data: bytes):
        """
        @brief Message constructor.

        @param[in] data Binary data that composes the message.
        """
        self.data = data
        self.length = len(data)


class Observer:
    """ 
    Represents an observer that reacts to received messages. 
    """

    def __init__(self, name: str, callback):
        """
        @brief Observer constructor.

        @param[in] name Name of the observer.
        @param[in] callback Callback function executed when a message is received.
        """
        self.name = name
        self.callback = callback


class SBus:
    """ 
    Represents a communication channel based on ZeroMQ. 
    """

    def __init__(self, name: str):
        """
        @brief SBus class constructor.

        @param[in] name Name of the channel.
        """
        self.name = name
        self.context = zmq.Context()
        self.publisher = None
        self.subscriber = None
        self.observers = []

        # Create directory if it does not exist
        os.makedirs("/tmp/feeds", exist_ok=True)

    def init_publisher(self):
        """
        @brief Initializes the channel as a publisher.
        """
        self.publisher = self.context.socket(zmq.PUB)
        endpoint = ENDPOINT_FORMAT.format(self.name)
        self.publisher.bind(endpoint)
        print(f"Publisher initialized at {endpoint}")

    def init_subscriber(self, observers: list):
        """
        @brief Initializes the channel as a subscriber.

        @param[in] observers List of observers that will react to received messages.
        """
        self.subscriber = self.context.socket(zmq.SUB)
        endpoint = ENDPOINT_FORMAT.format(self.name)
        self.subscriber.connect(endpoint)
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
        self.observers = observers
        print(f"Subscriber connected to {endpoint}")

    def send_message(self, message: Message):
        """
        @brief Sends a message through the channel.

        @param[in] message Message object containing the data to be sent.
        """
        if self.publisher:
            self.publisher.send(message.data)
            print(f"Message sent: {message.data.decode()}")

    def receive_message(self):
        """
        @brief Receives a message and notifies observers.
        """
        if self.subscriber:
            data = self.subscriber.recv()
            message = Message(data)

            for observer in self.observers:
                observer.callback(message)

    def destroy(self):
        """
        @brief Closes the sockets and terminates the ZeroMQ context.
        """
        if self.publisher:
            self.publisher.close()
        if self.subscriber:
            self.subscriber.close()
        self.context.term()
        print("Channel destroyed.")

