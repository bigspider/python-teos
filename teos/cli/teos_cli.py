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
from teos.cli.help import (
    show_usage,
    help_get_all_appointments,
    help_get_appointments,
    help_get_tower_info,
    help_get_users,
    help_get_user,
)


def make_rpc_request(rpc_url, method, *args):
    try:
        response = requests.post(
            url=rpc_url, json={"method": method, "params": args, "jsonrpc": "2.0", "id": uuid4().int}, timeout=5,
        )

        response_json = response.json()
        if response.status_code != constants.HTTP_OK:
            error = response_json["error"]
            if error:
                print(error.get("message"), file=sys.stderr)
                if error.get("data"):
                    print(json.dumps(error["data"], indent=4, sort_keys=True), file=sys.stderr)
                return
            else:
                print(
                    f"The server returned an error. Status code: {response.status_code}. Reason: {response.reason}",
                    file=sys.stderr,
                )

        print(json.dumps(response.json["result"], indent=4, sort_keys=True))

    except ConnectionError:
        print("Can't connect to the Eye of Satoshi. RPC server cannot be reached", file=sys.stderr)

    except requests.exceptions.Timeout:
        print("The request timed out", file=sys.stderr)


def get_all_appointments(rpc_url):
    """
    Gets information about all appointments stored in the tower.

    Args:
        rpc_url (:obj:`str`): the url of the teos RPC.
    """

    make_rpc_request(rpc_url, "get_all_appointments")


def get_appointments(rpc_url, locator):
    """
    Gets all the appointments for a specific locator.

    Args:
        rpc_url (:obj:`str`): the url of the teos RPC.
        locator (:obj:`str`): the locator of the requested appointment.
    """

    make_rpc_request(rpc_url, "get_appointments", locator)


def get_tower_info(rpc_url):
    """
    Gets general information about the tower.

    Args:
        rpc_url (:obj:`str`): the url of the teos RPC.
    """

    make_rpc_request(rpc_url, "get_tower_info")


def get_users(rpc_url):
    """
    Gets the list of registered user ids from the tower.

    Args:
        rpc_url (:obj:`str`): the url of the teos RPC.
    """

    make_rpc_request(rpc_url, "get_users")


def get_user(rpc_url, user_id):
    """
    Gets information about a specific user.

    Args:
        rpc_url (:obj:`str`): the url of the teos RPC.
        user_id (:obj:`str`): the requested user_id.
    """

    make_rpc_request(rpc_url, "get_user", user_id)


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
            get_all_appointments(teos_rpc_url)

        elif command == "get_appointments":
            if not args:
                sys.exit("No locator was given")
            if len(args) > 1:
                sys.exit(f"Expected only one argument, not {len(args)}")

            get_appointments(teos_rpc_url, args[0])

        elif command == "get_tower_info":
            get_tower_info(teos_rpc_url)

        elif command == "get_users":
            get_users(teos_rpc_url)

        elif command == "get_user":
            if not args:
                sys.exit("No user_id was given")
            if len(args) > 1:
                sys.exit(f"Expected only one argument, not {len(args)}")

            get_user(teos_rpc_url, args[0])

        elif command == "help":
            if args:
                command = args.pop(0)

                if command == "get_all_appointments":
                    sys.exit(help_get_all_appointments())

                elif command == "get_appointments":
                    sys.exit(help_get_appointments())

                elif command == "get_tower_info":
                    sys.exit(help_get_tower_info())

                elif command == "get_users":
                    sys.exit(help_get_users())

                elif command == "get_user":
                    sys.exit(help_get_user())

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
    commands = ["get_all_appointments", "get_appointments", "get_tower_info", "get_users", "get_user", "help"]

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
