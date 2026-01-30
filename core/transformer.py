"""Abstract transformers for DataFrame transformations."""

class Transformer:
    def transform(self, df):
        raise NotImplementedError()
