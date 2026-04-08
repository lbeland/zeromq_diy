## Install requirements
pip install -r requirements.txt
sudo apt-get install libzmq3-dev


## Start Python benchmark:
sudo python3 benchmark.py

## Start C benchmark:
make
sudo python3 c_benchmark.py