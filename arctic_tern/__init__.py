try:
    # First attempt to use asyncpg
    from ._migrate_asyncpg import migrate, migrate_multi
except ImportError:
    try:
        # If that fails try using psycopg
        from ._migrate_psycopg import migrate, migrate_multi
    except ImportError:
        raise ImportError("arctic_tern requires asyncpg or psycopg to function")
