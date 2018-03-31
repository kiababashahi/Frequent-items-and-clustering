import subprocess, os

def test_first_iter():
    command="python ./answers/first_iter.py ./data/plants11.data 3 123"
    process = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    code=process.wait()
    assert(not code), "Command failed"
    print(process.stdout.read().decode("utf-8"))
    assert(process.stdout.read().decode("utf-8")==open("tests/first_iter1.txt","r").read())
