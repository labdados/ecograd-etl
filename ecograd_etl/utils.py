import os
import sys
import urllib.request

def download_file(url, output_dir):
    print("Downloading from {} to {}".format(url, output_dir))
    output_filename = url.split('/')[-1]
    output_path = os.path.join(output_dir, output_filename)
    urllib.request.urlretrieve(url, filename=output_path)

def main(args):
    download_file(args[0], args[1])

if __name__ == '__main__':
    main(sys.argv[1:])