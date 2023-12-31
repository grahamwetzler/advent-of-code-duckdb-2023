def pip_wtf(command):
    """https://pip.wtf/"""
    import os
    import os.path
    import sys

    t = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        ".pip_wtf." + os.path.basename(__file__),
    )
    sys.path = [p for p in sys.path if "-packages" not in p] + [t]
    os.environ["PATH"] += os.pathsep + t + os.path.sep + "bin"
    os.environ["PYTHONPATH"] = os.pathsep.join(sys.path)
    if os.path.exists(t):
        return
    os.system(" ".join([sys.executable, "-m", "pip", "install", "-t", t, command]))
