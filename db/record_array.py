import numpy as np

class RecordArray:
    """Numpy array wrapper. Not really important."""
    items: np.array

    def __init__(self, array: np.ndarray) -> None:
        self.items = array
    
    def __getitem__(self, index: int):
        return self.items[index]

    def __bool__(self):
        return self.items is not None