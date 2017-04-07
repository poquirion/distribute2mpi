# distribute2mpi
Batch processor for localhost or HPC system based on mpi

It offers an interface similar to the one of multiprocessing Pool to run python method in parallel. 

Righ now a simple map method is available.

Usage:
```python
import distribute2mpi

def a_func(some_input):
    do_stuff()

pool = poneyexpress.MpiPool(n_proc=2)

async_results = pool.map_async(a_func, [input_1, input_2])

pool.join()

results = async_results.get()

```

The module distribute2mpi is developed to be the parralel computing tool of the SpinalCordToolbox https://github.com/neuropoly/spinalcordtoolbox. 
It is built around mpi4py https://pypi.python.org/pypi/mpi4py.  
