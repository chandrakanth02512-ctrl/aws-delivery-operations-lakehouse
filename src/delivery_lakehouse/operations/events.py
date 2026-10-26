"""Optional lifecycle observations shared by the existing snapshot and CDC runners."""


def emit(observer, stage, status, **details):
    if observer:
        observer(stage, status, details)
