from typing import List

from flask import Flask
from flask_jsonrpc import JSONRPC

from common.appointment import Appointment
from common.cryptographer import Cryptographer
from common.logger import get_logger


class RPC:
    """
    The :class:`RPC` exposes admin functionality of the watchtower.

    Args:
        host (:obj:`str`): the hostname to listen on.
        port (:obj:`int`): the port of the webserver.
        rw_lock (:obj:`RWLockWrite <readwritelock.rwlock.RWLockWrite>`): lock that must be acquired before reading or
            writing to the watchtower's state.
        inspector (:obj:`Inspector <teos.inspector.Inspector>`): an ``Inspector`` instance to check the correctness of
            the received appointment data.
        watcher (:obj:`Watcher <teos.watcher.Watcher>`): a ``Watcher`` instance to pass the requests to.
        responder (:obj:`Watcher <teos.responder.Responder>`): a ``Responder`` instance to pass the requests to.

    Attributes:
        logger: the logger for this component.
    """

    def __init__(self, host, port, rw_lock, inspector, watcher, responder):
        app = Flask(__name__)
        jsonrpc = JSONRPC(app, "/rpc", enable_web_browsable_api=True)
        self.app = app
        self.jsonrpc = jsonrpc

        self.logger = get_logger(component=RPC.__name__)

        self.host = host
        self.port = port
        self.rw_lock = rw_lock
        self.inspector = inspector
        self.watcher = watcher
        self.responder = responder
        self.logger.info("Initialized")

        @jsonrpc.method("get_all_appointments")
        def get_all_appointments() -> dict:
            return self.get_all_appointments()

        @jsonrpc.method("get_appointments")
        def get_appointments(locator: str) -> List[dict]:
            return self.get_appointments(locator)

        @jsonrpc.method("get_tower_info")
        def get_tower_info() -> dict:
            return self.get_tower_info()

        @jsonrpc.method("get_users")
        def get_users() -> List[str]:
            return self.get_users()

        @jsonrpc.method("get_user")
        def get_user(user_id: str) -> dict:
            return self.get_user(user_id)

    def start(self):
        """ This function starts the Flask server used to run the RPC """

        # ToDo: #185-serve-teosd-production
        self.app.run(host=self.host, port=self.port)

    def get_all_appointments(self):
        """
        Gives information about all the appointments in the Watchtower.

        Returns:
            :obj:`str`: A dictionary containing all the appointments hold by the ``Watcher``
            (``watcher_appointments``) and by the ``Responder`` (``responder_trackers``).
        """

        # ToDo: #15-add-system-monitor

        with self.rw_lock.gen_rlock():
            watcher_appointments = self.watcher.db_manager.load_watcher_appointments()
            responder_trackers = self.watcher.db_manager.load_responder_trackers()

        return {"watcher_appointments": watcher_appointments, "responder_trackers": responder_trackers}

    def get_appointments(self, locator):
        """
        Returns all the appointments with the given locator.

        Args:
            locator (:obj:`str`): the locator of the requested appointments.

        Returns:
            :obj:`list`: a list of dictionaries; each dictionary containes the "appointment" and "status" fields, that
            contain all the information for one of the appointments stored in the tower for the requested locator.
        """
        with self.rw_lock.gen_rlock():
            self.inspector.check_locator(locator)
            uuids = self.watcher.locator_uuid_map.get(locator)
            results = []

            for uuid in uuids:
                if uuid in self.watcher.appointments:
                    appointment_data = self.watcher.db_manager.load_watcher_appointment(uuid)
                    status = "being_watched"
                elif uuid in self.responder.trackers:
                    appointment_data = self.responder.db_manager.load_responder_tracker(uuid)
                    status = "dispute_responded"
                else:
                    raise AppointmentNotFound("Cannot find {}".format(locator))
                results.append({"appointment": appointment_data, "status": status})

            return results

    def get_tower_info(self):
        """
        Gives generic information about the watchtower.

        Returns:
            :obj:`str`: A dictionary containing information about the watchtower (TODO).
        """

        with self.rw_lock.gen_rlock():
            n_watcher_appointments = len(self.watcher.appointments)
            n_responder_trackers = len(self.responder.trackers)
            n_registered_users = len(self.watcher.gatekeeper.registered_users)
            tower_id = Cryptographer.get_compressed_pk(self.watcher.signing_key.public_key)

        return {
            "tower_id": tower_id,
            "n_registered_users": n_registered_users,
            "n_watcher_appointments": n_watcher_appointments,
            "n_responder_trackers": n_responder_trackers,
        }

    def get_users(self):
        """
        Returns the list of registered users.

        Returns:
            :obj:`list`: A list of the registered user_ids.
        """

        with self.rw_lock.gen_rlock():
            return list(self.watcher.gatekeeper.registered_users.keys())

    def get_user(self, user_id):
        """
        Returns information about a specific user.

        Args:
            user_id (:obj:`str`): the user_id of the requested user.

        Returns:
            :obj:`dict`: the information about the requested user.
        """

        # TODO: what to do if there's no such user?
        with self.rw_lock.gen_rlock():
            return self.watcher.gatekeeper.registered_users[user_id]
