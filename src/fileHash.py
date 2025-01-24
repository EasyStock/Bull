import os
import hashlib

def hash_file(file_path):
    """ SHA-256 """ 
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        hasher.update(f.read())
        return hasher.hexdigest()


def deduplicate_files(directory):
    """ """
    seen_hashes = {}
    for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                file_hash = hash_file(file_path)
                if file_hash in seen_hashes:
                    if "(1)" in file_path:
                        print(f"remove file: Duplicate file: {file_path}         {seen_hashes[file_hash]}")
                        os.remove(file_path)  #
                        continue

                    if "(1)" in seen_hashes[file_hash]:
                        print(f"remove file: Duplicate file: {seen_hashes[file_hash]}          {file_path}")
                        os.remove(seen_hashes[file_hash])  #
                        continue

                    print(f"warning: not remove  Duplicate file: {seen_hashes[file_hash]}          {file_path}")
                else:
                    seen_hashes[file_hash] = file_path


if __name__ == "__main__":
    target_directory = "/vol2/1000/EBook/500本电子书/" # NAS 
    deduplicate_files(target_directory)