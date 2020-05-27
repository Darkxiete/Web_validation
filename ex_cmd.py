from subprocess import Popen
import os.path

def ex_cmd(cmd: str, py_path=None) -> None:
    print(cmd)
    if "python" in cmd and py_path:
        cmd = cmd.replace("python", py_path)
    Popen(cmd, shell=True, env={"Path": os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, "bin"))}).communicate()


if __name__ == '__main__':
    print(os.path.join(os.path.dirname(__file__)))
    print(os.path.pardir)