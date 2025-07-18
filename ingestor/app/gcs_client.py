from google.cloud import storage


class GCSClientSingleton:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            cls._client = storage.Client()
        return cls._client
