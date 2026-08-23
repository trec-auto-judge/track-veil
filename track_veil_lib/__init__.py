"""Track Veil - Data Anonymization Library.

The version lives in pyproject.toml alone. It used to be duplicated here for hatch to
read, which is how it went stale at 0.1.0 while the CLI reported 0.4.5; read it from
the installed metadata instead:

    from importlib.metadata import version
    version("track_veil")
"""
