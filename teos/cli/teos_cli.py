#!/usr/bin/env python3

import sys
import json
import requests
from sys import argv
from getopt import getopt, GetoptError
from requests import ConnectionError
from uuid import uuid4

from common import constants
from common.config_loader import ConfigLoader
from common.tools import setup_data_folder
from common.exceptions import InvalidKey, InvalidParameter, SignatureError, TowerResponseError

from teos import DEFAULT_CONF, DATA_DIR, CONF_FILE_NAME
from teos.cli.help import show_usage, help_get_all_appointments


def make_rpc_request(rpc_url, method, *args):
    try:
        response = requests.post(
            url=rpc_url, json={"method": method, "params": args, "jsonrpc": "2.0", "id": uuid4().int}, timeout=5,
        )
        if response.status_code != constants.HTTP_OK:
            print(
                f"The server returned an error. Status code: {response.status_code}. Reason: {response.reason}",
                file=sys.stderr,
            )
            return None

        response_json = json.dumps(response.json()["result"], indent=4, sort_keys=True)
        return response_json

    except ConnectionError:
        print("Can't connect to the Eye of Satoshi. RPC server cannot be reached", file=sys.stderr)
        return None

    except requests.exceptions.Timeout:
        print("The request timed out", file=sys.stderr)
        return None


def get_all_appointments(rpc_url):
    """
    Gets information about all appointments stored in the tower, if the user requesting the data is an administrator.

    Args:
        rpc_url (:obj:`str`): the url of the teos RPC.

    Returns:
        :obj:`dict` a dictionary containing all the appointments stored by the Responder and Watcher if the tower
        responds.
    """

    return make_rpc_request(rpc_url, "get_all_appointments")


def get_tower_info(rpc_url):
    """
    Gets general information about the tower.

    Args:
        rpc_url (:obj:`str`): the url of the teos RPC.

    Returns:
        :obj:`dict` a dictionary containing info about the tower (TODO).
    """

    return make_rpc_request(rpc_url, "get_tower_info")


def main(command, args, command_line_conf):
    # Loads config and sets up the data folder and log file
    config_loader = ConfigLoader(DATA_DIR, CONF_FILE_NAME, DEFAULT_CONF, command_line_conf)
    config = config_loader.build_config()

    setup_data_folder(DATA_DIR)

    # Set the teos url
    teos_rpc_url = "{}:{}/rpc".format(config.get("RPC_BIND"), config.get("RPC_PORT"))

    # If an http or https prefix if found, leaves the server as is. Otherwise defaults to http.
    if not teos_rpc_url.startswith("http"):
        teos_rpc_url = "http://" + teos_rpc_url

    try:
        if command == "get_all_appointments":
            appointment_data = get_all_appointments(teos_rpc_url)
            if appointment_data:
                print(appointment_data)

        if command == "get_tower_info":
            appointment_data = get_tower_info(teos_rpc_url)
            if appointment_data:
                print(appointment_data)

        elif command == "help":
            if args:
                command = args.pop(0)

                if command == "get_all_appointments":
                    sys.exit(help_get_all_appointments())

                if command == "get_all_appointments":
                    sys.exit(help_get_tower_info())

                else:
                    sys.exit("Unknown command. Use help to check the list of available commands")

            else:
                sys.exit(show_usage())

    except (FileNotFoundError, IOError, ConnectionError, ValueError) as e:
        sys.exit(str(e))
    except (InvalidKey, InvalidParameter, TowerResponseError, SignatureError) as e:
        sys.exit(f"{e.msg}. Error arguments: {e.kwargs}")
    except Exception as e:
        sys.exit(f"Unknown error occurred: {str(e)}")


if __name__ == "__main__":
    command_line_conf = {}
    commands = ["get_all_appointments", "get_tower_info", "help"]

    try:
        opts, args = getopt(argv[1:], "h", ["rpcbind=", "rpcport=", "help"])

        for opt, arg in opts:
            if opt in ["--rpcbind"]:
                if arg:
                    command_line_conf["RPC_BIND"] = arg

            if opt in ["--rpcport"]:
                if arg:
                    try:
                        command_line_conf["RPC_PORT"] = int(arg)
                    except ValueError:
                        sys.exit("port must be an integer")

            if opt in ["-h", "--help"]:
                sys.exit(show_usage())

        command = args.pop(0) if args else None
        if command in commands:
            main(command, args, command_line_conf)
        elif not command:
            sys.exit("No command provided. Use help to check the list of available commands")
        else:
            sys.exit("Unknown command. Use help to check the list of available commands")

    except GetoptError as e:
        sys.exit("{}".format(e))
