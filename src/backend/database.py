"""
MongoDB database configuration and setup for Mergington High School API
"""

import copy
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from argon2 import PasswordHasher, exceptions as argon2_exceptions

logger = logging.getLogger(__name__)


class _UpdateResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


def _get_nested_value(document, field_path):
    """Resolve dotted path values from nested dicts."""
    value = document
    for part in field_path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def _matches_query(document, query):
    for key, expected in query.items():
        actual = _get_nested_value(document, key)
        if isinstance(expected, dict):
            for operator, operand in expected.items():
                if operator == "$in":
                    if isinstance(actual, list):
                        if not any(item in actual for item in operand):
                            return False
                    elif actual not in operand:
                        return False
                elif operator == "$gte":
                    if actual is None or actual < operand:
                        return False
                elif operator == "$lte":
                    if actual is None or actual > operand:
                        return False
                else:
                    return False
        else:
            if actual != expected:
                return False
    return True


class InMemoryCollection:
    """Tiny subset of a Mongo collection API used by this project."""

    def __init__(self):
        self._documents = []

    def count_documents(self, query):
        return sum(1 for doc in self._documents if _matches_query(doc, query))

    def insert_one(self, document):
        self._documents.append(copy.deepcopy(document))

    def find(self, query=None):
        query = query or {}
        for document in self._documents:
            if _matches_query(document, query):
                # Return copies so route-level pop() does not mutate storage.
                yield copy.deepcopy(document)

    def find_one(self, query):
        for document in self.find(query):
            return document
        return None

    def update_one(self, query, update):
        for idx, document in enumerate(self._documents):
            if not _matches_query(document, query):
                continue

            updated = copy.deepcopy(document)
            modified = 0

            for operator, payload in update.items():
                if operator == "$push":
                    for field, value in payload.items():
                        if field not in updated:
                            updated[field] = []
                        if not isinstance(updated[field], list):
                            continue
                        updated[field].append(value)
                        modified += 1
                elif operator == "$pull":
                    for field, value in payload.items():
                        if field in updated and isinstance(updated[field], list) and value in updated[field]:
                            updated[field].remove(value)
                            modified += 1

            if modified > 0:
                self._documents[idx] = updated
            return _UpdateResult(modified_count=modified)

        return _UpdateResult(modified_count=0)

    def aggregate(self, pipeline):
        """Supports only the exact pipeline used by /activities/days."""
        docs = [copy.deepcopy(doc) for doc in self._documents]

        for stage in pipeline:
            if "$unwind" in stage:
                field_path = stage["$unwind"].lstrip("$")
                unwound = []
                for doc in docs:
                    values = _get_nested_value(doc, field_path)
                    if not isinstance(values, list):
                        continue
                    for value in values:
                        new_doc = copy.deepcopy(doc)
                        target = new_doc
                        parts = field_path.split(".")
                        for part in parts[:-1]:
                            target = target.get(part, {})
                        target[parts[-1]] = value
                        unwound.append(new_doc)
                docs = unwound
            elif "$group" in stage:
                group_field = stage["$group"]["_id"].lstrip("$")
                grouped_values = sorted({
                    _get_nested_value(doc, group_field)
                    for doc in docs
                    if _get_nested_value(doc, group_field) is not None
                })
                docs = [{"_id": value} for value in grouped_values]
            elif "$sort" in stage:
                sort_fields = stage["$sort"]
                if "_id" in sort_fields:
                    reverse = sort_fields["_id"] < 0
                    docs = sorted(docs, key=lambda d: d.get("_id"), reverse=reverse)

        for doc in docs:
            yield doc


def _create_collections():
    """Connect to MongoDB, or gracefully fallback to in-memory collections."""
    try:
        mongo_client = MongoClient(
            "mongodb://localhost:27017/",
            serverSelectionTimeoutMS=1500,
        )
        # Force a connection attempt at startup.
        mongo_client.admin.command("ping")
        mongo_db = mongo_client["mergington_high"]
        logger.info("Connected to MongoDB at localhost:27017")
        return mongo_client, mongo_db["activities"], mongo_db["teachers"]
    except PyMongoError as exc:
        logger.warning(
            "MongoDB unavailable (%s). Falling back to in-memory datastore.",
            exc,
        )
        return None, InMemoryCollection(), InMemoryCollection()


# Connect to MongoDB or fallback to in-memory storage.
client, activities_collection, teachers_collection = _create_collections()

# Methods


def hash_password(password):
    """Hash password using Argon2"""
    ph = PasswordHasher()
    return ph.hash(password)


def verify_password(hashed_password: str, plain_password: str) -> bool:
    """Verify a plain password against an Argon2 hashed password.

    Returns True when the password matches, False otherwise.
    """
    ph = PasswordHasher()
    try:
        ph.verify(hashed_password, plain_password)
        return True
    except argon2_exceptions.VerifyMismatchError:
        return False
    except Exception:
        # For any other exception (e.g., invalid hash), treat as non-match
        return False


def init_database():
    """Initialize database if empty"""

    # Initialize activities if empty
    if activities_collection.count_documents({}) == 0:
        for name, details in initial_activities.items():
            activities_collection.insert_one({"_id": name, **details})

    # Initialize teacher accounts if empty
    if teachers_collection.count_documents({}) == 0:
        for teacher in initial_teachers:
            teachers_collection.insert_one(
                {"_id": teacher["username"], **teacher})


# Initial database if empty
initial_activities = {
    "Chess Club": {
        "description": "Learn strategies and compete in chess tournaments",
        "schedule": "Mondays and Fridays, 3:15 PM - 4:45 PM",
        "schedule_details": {
            "days": ["Monday", "Friday"],
            "start_time": "15:15",
            "end_time": "16:45"
        },
        "max_participants": 12,
        "participants": ["michael@mergington.edu", "daniel@mergington.edu"]
    },
    "Programming Class": {
        "description": "Learn programming fundamentals and build software projects",
        "schedule": "Tuesdays and Thursdays, 7:00 AM - 8:00 AM",
        "schedule_details": {
            "days": ["Tuesday", "Thursday"],
            "start_time": "07:00",
            "end_time": "08:00"
        },
        "max_participants": 20,
        "participants": ["emma@mergington.edu", "sophia@mergington.edu"]
    },
    "Morning Fitness": {
        "description": "Early morning physical training and exercises",
        "schedule": "Mondays, Wednesdays, Fridays, 6:30 AM - 7:45 AM",
        "schedule_details": {
            "days": ["Monday", "Wednesday", "Friday"],
            "start_time": "06:30",
            "end_time": "07:45"
        },
        "max_participants": 30,
        "participants": ["john@mergington.edu", "olivia@mergington.edu"]
    },
    "Soccer Team": {
        "description": "Join the school soccer team and compete in matches",
        "schedule": "Tuesdays and Thursdays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Tuesday", "Thursday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 22,
        "participants": ["liam@mergington.edu", "noah@mergington.edu"]
    },
    "Basketball Team": {
        "description": "Practice and compete in basketball tournaments",
        "schedule": "Wednesdays and Fridays, 3:15 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Wednesday", "Friday"],
            "start_time": "15:15",
            "end_time": "17:00"
        },
        "max_participants": 15,
        "participants": ["ava@mergington.edu", "mia@mergington.edu"]
    },
    "Art Club": {
        "description": "Explore various art techniques and create masterpieces",
        "schedule": "Thursdays, 3:15 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Thursday"],
            "start_time": "15:15",
            "end_time": "17:00"
        },
        "max_participants": 15,
        "participants": ["amelia@mergington.edu", "harper@mergington.edu"]
    },
    "Drama Club": {
        "description": "Act, direct, and produce plays and performances",
        "schedule": "Mondays and Wednesdays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Monday", "Wednesday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 20,
        "participants": ["ella@mergington.edu", "scarlett@mergington.edu"]
    },
    "Math Club": {
        "description": "Solve challenging problems and prepare for math competitions",
        "schedule": "Tuesdays, 7:15 AM - 8:00 AM",
        "schedule_details": {
            "days": ["Tuesday"],
            "start_time": "07:15",
            "end_time": "08:00"
        },
        "max_participants": 10,
        "participants": ["james@mergington.edu", "benjamin@mergington.edu"]
    },
    "Debate Team": {
        "description": "Develop public speaking and argumentation skills",
        "schedule": "Fridays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Friday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 12,
        "participants": ["charlotte@mergington.edu", "amelia@mergington.edu"]
    },
    "Weekend Robotics Workshop": {
        "description": "Build and program robots in our state-of-the-art workshop",
        "schedule": "Saturdays, 10:00 AM - 2:00 PM",
        "schedule_details": {
            "days": ["Saturday"],
            "start_time": "10:00",
            "end_time": "14:00"
        },
        "max_participants": 15,
        "participants": ["ethan@mergington.edu", "oliver@mergington.edu"]
    },
    "Science Olympiad": {
        "description": "Weekend science competition preparation for regional and state events",
        "schedule": "Saturdays, 1:00 PM - 4:00 PM",
        "schedule_details": {
            "days": ["Saturday"],
            "start_time": "13:00",
            "end_time": "16:00"
        },
        "max_participants": 18,
        "participants": ["isabella@mergington.edu", "lucas@mergington.edu"]
    },
    "Sunday Chess Tournament": {
        "description": "Weekly tournament for serious chess players with rankings",
        "schedule": "Sundays, 2:00 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Sunday"],
            "start_time": "14:00",
            "end_time": "17:00"
        },
        "max_participants": 16,
        "participants": ["william@mergington.edu", "jacob@mergington.edu"]
    }
}

initial_teachers = [
    {
        "username": "mrodriguez",
        "display_name": "Ms. Rodriguez",
        "password": hash_password("art123"),
        "role": "teacher"
    },
    {
        "username": "mchen",
        "display_name": "Mr. Chen",
        "password": hash_password("chess456"),
        "role": "teacher"
    },
    {
        "username": "principal",
        "display_name": "Principal Martinez",
        "password": hash_password("admin789"),
        "role": "admin"
    }
]
