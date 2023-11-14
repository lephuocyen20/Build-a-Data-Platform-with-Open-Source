import os
from typing import Optional

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

ENABLE_PROXY_FIX = True
SECRET_KEY = '7/rWpXFGCHoqsEhUgxtPCg=='


def get_env_variable(var_name: str, default: Optional[str] = None) -> str:
    """Get the environment variable or raise exception."""
    try:
        return os.environ[var_name]
    except KeyError:
        if default is not None:
            return default
        else:
            error_msg = "The environment variable {} was missing, abort...".format(
                var_name
            )
            raise EnvironmentError(error_msg)

DATABASE_DIALECT = get_env_variable("DATABASE_DIALECT")
DATABASE_USER = get_env_variable("POSTGRES_USER")
DATABASE_PASSWORD = get_env_variable("POSTGRES_PASSWORD")
DATABASE_HOST = get_env_variable("POSTGRES_HOSTS")
DATABASE_PORT = get_env_variable("POSTGRES_PORT")
DATABASE_DB = get_env_variable("POSTGRES_DBS")

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = "%s://%s:%s@%s:%s/%s" % (
    DATABASE_DIALECT,
    DATABASE_USER,
    DATABASE_PASSWORD,
    DATABASE_HOST,
    DATABASE_PORT,
    DATABASE_DB,
)
