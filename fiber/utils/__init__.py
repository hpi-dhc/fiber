from .timer import Timer


def isnotebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter


if isnotebook():
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm

__all__ = [
    'Timer',
    'tqdm'
]
