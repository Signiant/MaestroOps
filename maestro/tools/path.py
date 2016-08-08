import os, platform, re, shutil

def get_tree_size(path = '.'):
    """
    get_tree_size will return the total size of a directory tree
    """
    if not os.path.exists(path):
        raise OSError("Path " + str(path) + " does not exist!")

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(str(path)):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def get_case_insensitive_path(path = '.'):
    """
    get_case_insensitive_path will check for the existance of a path in a case sensitive file system, regardless of the case of the inputted path. Returns the absolute path if found (with correct casing) or None.
    """
    if os.path.exists(path):
        return path
    elif platform.system() == "Windows":
        return None

    path_elements = full_split(path)
    path_root = None

    drive, path = os.path.splitdrive(path)
    if not drive:
        if not path.startswith("/"):
            path_root = os.path.abspath(os.path.normpath("./"))
        else:
            path_root = os.path.abspath(os.path.normpath("/"))
    else:
        path_root = os.path.abspath(os.path.normpath(drive))

    if not os.path.exists(path_root):
        raise OSError("Unable to locate path root: " + str(path_root))

    #Build the full path, also used for error messages
    full_path = path_root
    for element in path_elements:
        if not element or element == "/" or element == ".":
            continue
        found = False
        for directory in os.listdir(full_path):
            if element.lower() == directory.lower():
                full_path = os.path.join(full_path,directory)
                found = True
                break
        if found is False:
            return None
    return full_path

# Credit: Gian Marco Gherardi
#         http://stackoverflow.com/questions/6260149/os-symlink-support-in-windows
def symlink(source, link_name):
    import os
    os_symlink = getattr(os, "symlink", None)
    if callable(os_symlink):
        os_symlink(source, link_name)
    else:
        import ctypes
        csl = ctypes.windll.kernel32.CreateSymbolicLinkW
        csl.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        csl.restype = ctypes.c_ubyte
        flags = 1 if os.path.isdir(source) else 0
        if csl(link_name, source, flags) == 0:
            raise ctypes.WinError()

def purge(pattern, path, match_directories = False):
    for root, dirs, files in os.walk(path):
        if match_directories is True:
            for dir in filter(lambda x: re.match(pattern, x), dirs):
                shutil.rmtree(os.path.join(root,dir))
        for file in filter(lambda x: re.match(pattern, x), files):
            os.remove(os.path.join(root, file))

# Credit: John Machin
#         http://stackoverflow.com/questions/4579908/cross-platform-splitting-of-path-in-python
def full_split(path, debug=False):
    """
    full_split will split Windows and UNIX paths into seperate elements
    """
    parts = []
    while True:
        newpath, tail = os.path.split(path)
        if debug: print (repr(path), (newpath, tail))
        if newpath == path:
            assert not tail
            if path: parts.append(path)
            break
        parts.append(tail)
        path = newpath
    parts.reverse()
    return parts
