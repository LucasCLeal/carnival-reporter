"""
Documentation

See also https://www.python-boilerplate.com/flask
"""
import os
from flask import Flask, Response
from helpers.kafkaAgent import KafkaAgent


def create_app(config=None):
    app = Flask(__name__)

    # Definition of the routes. Put them into their own file. See also

    @app.route("/")
    def hello_world():
        return "Hello World"

    @app.route("/process")
    def proccess():
        return Response("sucesso", status=200, mimetype='text/plain')

    @app.route("/start_agent")
    def start_agent():
        # agent creation
        smith = KafkaAgent()
        smith.start("beta_input_topic")
        return "it has started"

    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app = create_app()
    app.run(host="0.0.0.0", port=port)