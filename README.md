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

To execute script using distribute2mpi on a single machine, just run `mpiexec -n 1 my_script.py --some_script_option --more_options`

To run your code on an HPC running for example PBS, run `qsub submit_myscript.sh`, where submit_myscript.sh is:

```bash
#!/bin/bash
#PBS -l procs=1000 
#PBS -N THIS_IS_USING_A_LOT_OF_RESSOURSES

mpiexec -n 1 my_script.py --some_script_option --more_options
```


--------------------------------------------
The module distribute2mpi is developed to be the parralel computing tool of the SpinalCordToolbox https://github.com/neuropoly/spinalcordtoolbox. 
It is built around mpi4py https://pypi.python.org/pypi/mpi4py.  
