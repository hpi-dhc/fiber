import sys

from .timer import Timer

if 'ipykernel' in sys.modules:
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm

__all__ = [
    'Timer',
    'tqdm'
]
