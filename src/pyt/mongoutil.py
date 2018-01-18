"""
PyMongo-based utilities for dealing with MongoDB.

The module requires `PyMongo` to be installed.
"""


# Imports
# ----------------------------------------


from functools import lru_cache
from functools import wraps

from bson.objectid import ObjectId

from pymongo import MongoClient
from pymongo import IndexModel
from pymongo.errors import ConnectionFailure
from pymongo.errors import InvalidName


# Metadata
# ----------------------------------------


from typing import TYPE_CHECKING, Callable, Dict, Generic, List, Optional, Type, TypeVar, Union
if TYPE_CHECKING:
    from pymongo.collection import Collection
    from pymongo.database import Database
    from pymongo.results import InsertManyResult
    from pymongo.results import InsertOneResult


# Information
# ------------------------------------------------------------


__author__ = 'Peter Volf'


# Public methods
# ----------------------------------------


def autoretry(method: Callable) -> Callable:
    """
    Decorator that can be used to auto-retry database operations.

    Keep in mind that in MongoDB there are no transactions, so using this decorator
    on "slow" methods that execute more than one database operation can be dangerous
    if a connection failure happens between two operations, because the already
    executed operations will also get executed again.

    Retrying an operation only helps if the primary or master server becomes
    unavailable but at least one secondary or replica server is still available.
    In this case one retry is enough because during the second attempt a new
    primary server will be elected and the database operation will then succeed.
    If the retried operation also fails, then the decorated method's `ConnectionFailure`
    exception will not be caught by the decorator.

    Arguments:
        method [Callable]: The MongoDB operation to retry automatically.

    Returns:
        The decorated method.
    """
    @wraps(method)
    def wrapped(*args, **kwargs):
        try:
            # Try to execute the operation.
            return method(*args, **kwargs)
        except ConnectionFailure:
            # Try one more time. Hopefully a new primary server has already been
            # elected and the operation will succeed. If not, then
            return method(*args, **kwargs)

    return wrapped


def check_connection(client: MongoClient) -> None:
    """
    Checks whether the given `MongoClient` is connected to a `mongod` server instance.

    Arguments:
        client (MongoClient): The client to check.

    Raises:
        ConnectionFailure: If the client is not connected.
    """
    # Test whether the client is connected to a server.
    # Notes:
    #  - "admin" is a database that is always available.
    #  - The "ismaster" command is cheap and doesn't require authentication.
    client.admin.command("ismaster")


@lru_cache(3)
def get_client(url: str, port: int) -> Optional[MongoClient]:
    """
    Returns a `MongoClient` that is connected to specified `mongod` server
    or `None` if the connection couldn't be established. As a consequence,
    the method blocks for as long the connection is not established or
    it is concluded that it couldn't be established.

    The created clients are cached using a fairly small LRU cache, still
    under normal circumstances you can expect to get the same `MongoClient`
    instance if you call the method with the same arguments.

    Arguments:
        url (str): The URL of the MongoDB server to connect to.
        port (int): The port to use to connect to the server.

    Returns:
        A `MongoClient` instance connected to specified `mongod` server
        or `None` if the connection couldn't be established.
    """
    client: MongoClient = MongoClient(url, port)
    try:
        check_connection(client)
        return client
    except ConnectionFailure:
        return None


@lru_cache(27)
def get_collection(url: str,
                   port: int,
                   database_name: str,
                   collection_name: str) -> Optional["Collection"]:
    """
    Returns a pymongo handler for the specified collection or `None` if the
    connection couldn't be established with the MongoDB instance or the
    database name or collection name is invalid.

    The returned collection instances are cached using an LRU cache, so under
    normal circumstances you can expect to get the same collection instance
    if you call the method with the same arguments.

    Arguments:
        url (str): The URL of the MongoDB server to connect to.
        port (int): The port to use to connect to the server.
        database_name (str): The name of the database that contains the collection.
        collection_name (str): The name of the collection to get a handler for.

    Returns:
        A pymongo handler for the specified collection or `None` if the connection
        couldn't be established with the MongoDB instance or the database name or
        collection name is invalid.
    """
    database: Database = get_database(url, port, database_name)
    if database is None:
        return None

    try:
        return database[collection_name]
    except TypeError:
        return None
    except InvalidName:
        return None


@lru_cache(9)
def get_database(url: str, port: int, database_name: str) -> Optional["Database"]:
    """
    Returns a pymongo handler for the specified database of the given MongoDB
    instance or `None` if either the connection couldn't be established with
    the MongoDB instance or the database name is invalid.

    The returned database instances are cached using an LRU cache, so under
    normal circumstances you can expect to get the same database instance
    if you call the method with the same arguments.

    Arguments:
        url (str): The URL of the MongoDB server to connect to.
        port (int): The port to use to connect to the server.
        database_name (str): The name of the database to get a handler for.

    Returns:
        A pymongo handler for the specified database of the given MongoDB
        instance or `None` if either the connection couldn't be established with
        the MongoDB instance or the database name is invalid.
    """
    client: MongoClient = get_client(url, port)
    if client is None:
        return None

    try:
        return client[database_name]
    except InvalidName:
        return None
    except TypeError:
        return None


def is_connected(client: MongoClient) -> bool:
    """
    Returns whether the given client is connected to the server.

    This is convenience method for calling `check_connection()` without the fear
    of getting an exception.
    """
    try:
        check_connection(client)
        return True
    except ConnectionFailure:
        return False


# Classes
# ----------------------------------------


MongoDataType: TypeVar = TypeVar("MongoDataType", bound="MongoDocument")
"""
The base data type for `MongoDocument`s.
"""


class MongoDocument(object):
    """
    Abstract base class for classes that support both classic JSON externalization
    and MongoDB JSON externalization.
    """

    # Class methods
    # ----------------------------------------

    @classmethod
    def create_from_json(cls: Type[MongoDataType], data: Dict) -> MongoDataType:
        """
        Creates a new instance of the class and sets it up from the given data.

        Arguments:
            data (Dict): The JSON representation of the object.

        Returns:
            A new instance set up from the given data.
        """
        raise NotImplementedError("{} is abstract.".format(cls.__name__))

    @classmethod
    def create_from_mongo(cls: Type[MongoDataType], data: Dict) -> MongoDataType:
        """
        Creates a new instance of the class and sets it up from the given data.

        Arguments:
            data (Dict): The MongoDB JSON representation of the object.

        Returns:
            A new instance set up from the given data
        """
        raise NotImplementedError("{} is abstract.".format(cls.__name__))

    @classmethod
    def json_to_mongo(cls, data: Dict) -> None:
        """
        Converts the JSON representation of an instance of the class to its
        corresponding MongoDB JSON representation in-place.

        Arguments:
            data (Dict): The data to convert.
        """
        raise NotImplementedError("{} is abstract.".format(cls.__name__))

    @classmethod
    def mongo_to_json(cls, data: Dict) -> None:
        """
        Converts the MongoDB JSON representation of an instance of the class
        to its corresponding "classic" JSON representation in-place.

        Arguments:
            data (Dict): The data to convert.
        """
        raise NotImplementedError("{} is abstract.".format(cls.__name__))

    # Public methods
    # ----------------------------------------

    def to_json(self) -> Dict:
        """
        Returns the JSON representation of the object.

        Note that the method may modify the given data.

        This method should be the same as `to_mongo()` except it must convert BSON
        externalizable objects to valid JSON.
        """
        raise NotImplementedError("{} is abstract.".format(self.__class__.__name__))

    def from_json(self, data: Dict) -> None:
        """
        Restores the state of the object from its JSON description.

        Note that the method may modify the given data.

        Arguments:
            data (Dict): The JSON representation to restore the state of the object from.
        """
        raise NotImplementedError("{} is abstract.".format(self.__class__.__name__))

    def to_mongo(self) -> Dict:
        """
        Returns the MongoDB JSON representation of the object.

        MongoDB can directly handle `datetime.datetime` or `bson.objectid.ObjectID`
        objects for example, but that is obviously not allowed in classic JSON.
        Basically this method needs to return a BSON convertible object.
        """
        raise NotImplementedError("{} is abstract.".format(self.__class__.__name__))

    def from_mongo(self, data: Dict) -> None:
        """
        Restores the state of the object from its MongoDB JSON representation.
        """
        raise NotImplementedError("{} is abstract.".format(self.__class__.__name__))


class BaseDocument(MongoDocument):
    """
    Base implementation of `MongoDocument`.
    """

    # MongoDocument class methods
    # ----------------------------------------

    @classmethod
    def create_from_json(cls: Type[MongoDataType], data: Dict) -> MongoDataType:
        result: MongoDataType = cls()
        result.from_json(data)
        return result

    @classmethod
    def create_from_mongo(cls: Type[MongoDataType], data: Dict) -> MongoDataType:
        result: MongoDataType = cls()
        result.from_mongo(data)
        return result

    @classmethod
    def json_to_mongo(cls, data: Dict) -> None:
        # Don't modify data.
        pass

    @classmethod
    def mongo_to_json(cls, data: Dict) -> None:
        # Don't modify data.
        pass

    # MongoDocument methods
    # ----------------------------------------

    def to_json(self) -> Dict:
        result: Dict = self.to_mongo()
        self.__class__.mongo_to_json(result)
        return result

    def from_json(self, data: Dict) -> None:
        self.__class__.json_to_mongo(data)
        self.from_mongo(data)

    def to_mongo(self) -> Dict:
        return {}

    def from_mongo(self, data: Dict) -> None:
        # Do nothing.
        pass


class ServerDescriptor(object):
    """
    Descriptor that stores the basic information required to access a given MongoDB server.
    """

    # Initialization
    # ----------------------------------------

    def __init__(self, url: str, port: int):
        """
        Initialization.

        Arguments:
            url (str): The URL of the MongoDB server.
            port (int): The port to use to connect to the server.
        """
        self._url: str = url
        """
        The URL of the MongoDB server.
        """

        self._port: int = port
        """
        The port to use to connect to the server.
        """

    # Properties
    # ----------------------------------------

    @property
    def client(self) -> MongoClient:
        """
        The `MongoClient` used to connect to the MongoDB server.
        """
        return get_client(self._url, self._port)

    @property
    def is_connected(self) -> bool:
        """
        Whether the client is connected to the server.
        """
        return is_connected(self.client)

    @property
    def port(self) -> int:
        """
        The port to use to connect to the server.
        """
        return self._port

    @property
    def url(self) -> str:
        """
        The URL of the MongoDB server.
        """
        return self._url


class DatabaseDescriptor(object):
    """
    Descriptor that stores the basic information required to access a given database
    on a MongoDB server.
    """

    # Initialization
    # ----------------------------------------

    def __init__(self,
                 server: ServerDescriptor,
                 database_name: str,
                 username: str,
                 password: str) -> None:
        """
        Initialization.

        Authentication will be performed automatically when a new descriptor is created.

        Arguments:
            server (ServerDescriptor): The descriptor of the server that has the database.
            database_name (str): The name of the database.
            username (str): The username to access the database with.
            password (str): The password corresponding to the given username.
        """
        self._server: ServerDescriptor = server
        """
        The descriptor of the server that has the database.
        """

        self._database_name: str = database_name
        """
        The name of the referenced database.
        """

        self.database.authenticate(username, password)

    # Properties
    # ----------------------------------------

    @property
    def database(self) -> "Database":
        """
        The referenced database.
        """
        return self._server.client[self._database_name]

    @property
    def database_name(self) -> str:
        """
        The name of the referenced database.
        """
        return self._database_name

    @property
    def is_connected(self) -> bool:
        """
        Whether the client is connected to the server.
        """
        return self._server.is_connected


class CollectionDescriptor(Generic[MongoDataType]):
    """
    Descriptor that stores the basic information required to access a given collection
    of a database on a MongoDB server.
    """

    # Initialization
    # ----------------------------------------

    def __init__(self,
                 database: DatabaseDescriptor,
                 document_class: Type[MongoDocument],
                 collection_name: str) -> None:
        """
        Initialization.

        Authentication will be performed automatically when a new descriptor is created.

        Arguments:
            database (DatabaseDescriptor): The descriptor of the database that has the collection.
            document_class (Type[MongoDocument]): The class of the documents stored in the
                                                  collection. It must be the same as the
                                                  collection's generic type.
            collection_name (str): The name of the referenced collection.
        """
        self._database_descriptor: DatabaseDescriptor = database
        """
        The descriptor of the database that has the collection.
        """

        self._document_class: Type[MongoDocument] = document_class
        """
        The class of the documents stored in the collection.
        """

        self._collection_name: str = collection_name
        """
        The name of the referenced collection.
        """

        self._collection: Collection = self._database_descriptor.database[self._collection_name]
        """
        The referenced collection.
        """

    # Properties
    # ----------------------------------------

    @property
    def collection(self) -> "Collection":
        """
        The referenced collection.
        """
        return self._collection

    @property
    def collection_name(self) -> str:
        """
        The name of the referenced collection.
        """
        return self._collection_name

    @property
    def database_descriptor(self) -> DatabaseDescriptor:
        """
        The descriptor of the database that has the collection.
        """
        return self._database_descriptor

    @property
    def document_class(self) -> Type[MongoDocument]:
        """
        The class of the documents stored in the collection.
        """
        return self._document_class

    @property
    def is_connected(self) -> bool:
        """
        Whether the client is connected to the server.
        """
        return self._database_descriptor.is_connected

    # Public methods
    # ----------------------------------------

    @autoretry
    def find_one(self, query: Dict) -> Optional[MongoDataType]:
        """
        Returns the first document that matches the given query in the collection
        if at least one such document exists.

        Arguments:
            query (Dict): Dictionary that specifies the query.

        Returns:
            The first document that matches the query or `None` if no such document exists.
        """
        data: Dict = self._collection.find_one(query)
        return None if data is None else self._document_class.create_from_mongo(data)

    @autoretry
    def find_by_id(self, identifier: Union[ObjectId, str]) -> Optional[MongoDataType]:
        """
        Finds the MongoDB JSON document with the given identifier in the collection
        if such a document exists.

        Arguments:
            identifier (Union[ObjectId, str]): The identifier of the object to find,

        Returns:
            The document with the given identifier or `None`.
        """
        if isinstance(identifier, str):
            identifier = ObjectId(identifier)

        data: Dict = self._collection.find_one({"_id": identifier})
        return None if data is None else self._document_class.create_from_mongo(data)

    @autoretry
    def insert_one(self, item: MongoDataType) -> "InsertOneResult":
        """
        Inserts the given document into the collection.

        Arguments:
            item (MongoDataType): The document to insert.
        """
        return self._collection.insert_one(item.to_mongo())

    @autoretry
    def insert_many(self, items: List[MongoDataType]) -> "InsertManyResult":
        """
        Inserts the given list of items into the collection.

        Arguments:
            items (List[MongoDataType]): The documents to insert.
        """
        return self._collection.insert_many([item.to_mongo() for item in items])


class DevCollectionDescriptor(CollectionDescriptor, Generic[MongoDataType]):
    """
    `CollectionDescriptor` extension to use exclusively for development.
    """

    # Public methods
    # ----------------------------------------

    @autoretry
    def create_index(self, index: IndexModel) -> str:
        """
        Creates the specified index on the collection.

        This is a wrapper around PyMongo's `Collection.create_index()` method.

        Arguments:
            index (IndexModel): The index to create.

        Returns:
            The name of the created index.
        """
        return self._collection.create_index(index)

    @autoretry
    def create_indexes(self, indexes: List[IndexModel]) -> List[str]:
        """
        Creates the specified indexes on the collection.

        This is a wrapper around PyMongo's `Collection.create_indexes()` method.

        Arguments:
            indexes (IndexModel): The indexes to create.

        Returns:
            The names of the created indexes.
        """
        return self._collection.create_indexes(indexes)

    @autoretry
    def drop(self) -> None:
        """
        Drops the collection with all its indexes from the database.

        This is a wrapper around PyMongo's `Collection.drop()` method.
        """
        self._collection.drop()

    @autoretry
    def drop_indexes(self) -> None:
        """
        Drops all the indexes of the collection from the database.

        This is a wrapper around PyMongo's `Collection.drop_indexes()` method.
        """
        self._collection.drop_indexes()
