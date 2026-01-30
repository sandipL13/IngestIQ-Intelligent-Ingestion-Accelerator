"""Abstract loaders for targets."""

class Loader:
    def load(self, df):
        raise NotImplementedError()
