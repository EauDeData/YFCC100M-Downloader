import os
import urllib.request
from multiprocessing import Process, Queue, Manager
import pickle


def download_file(src, dst, filename):
    #print(type(dst), type(filename))
    if os.path.exists(dst + filename):
        return 0 
    try:
        
        urllib.request.urlretrieve(src, dst + filename)
        print(f"{src} Downloaded")
        return 0
    except Exception as e:
        print(f"{src} doesnt exist or exit error: {e}")
        return 1

def build_meta(meta_file, download = '/data/adria/YFCC/data_original/', csIndex = [8, 9], keepIndex = [3, 6, 7, 8, 9, 10, 11, 12, 14], keepNames = ['date', 'caption', 'description', 'user-tags', 'machine-tags', 'xCoord', 'yCoord', 'accuracy', 'download']):
    
    #### meta_file: path to the metta file from s3://mmcommons
    # csIndex: Comma Searated Values index from each line
    # keepIndex: Data i want to keep
    # keepNames: Names for the metadata i want to keep
    # download: if you want me to download the things
    handler = open(meta_file)
    
    def mp_builder(queue, handler, n_thread, meta_json = {}, download = download, csIndex = csIndex, keepIndex = keepIndex, keepNames = keepNames):

        
        n = 0
        for line in handler:

            if n%n_thread != 0:
                continue
            n += 1
            if (n + 1)%100 != 0:
                pickle.dump(meta_json, open(f'./meta_split_{n_thread}.pkl', 'wb'))


            line = line.split('\t')
            if len(line) != 23:
                continue
            _id = line[0]
            
            if _id in meta_json or line[-1] == "1":
                #line[-1] == "1"; I don't want videos >:(
                continue
            meta_json[_id] = {}
            for num, element in enumerate(line):
                if num in keepIndex:
                    if num in csIndex:
                        element = element.split(',')
                    name = keepIndex.index(num)
                    name = keepNames[name]
                    #print(f"{num}, {name}")
                    meta_json[_id][name] = element
            #print(meta_json[_id])
            img = meta_json[_id]['download'].split('/')[-1]
            if download:
                
                exit_ = download_file(meta_json[_id]['download'], download, img)
                if (exit_ == 0):
                    meta_json[_id]['filepath'] = download + img
                    meta_json[_id]['filename'] = img
        queue.append(meta_json)
    
    manager = Manager()
    queue = manager.list()

    threads = 8
    process = [Process(target = mp_builder, args = (queue, handler, x + 1)) for x in range(threads)]
    [x.start() for x in process]
    [x.join() for x in process]

    whole = {}
    for response in queue:
        whole = {**whole, **response}

    pickle.dump(whole, open('./meta.pkl', 'wb'))

    return whole

if __name__ == '__main__':
    #images = get_availible_images('/data/adria/YFCC/data/')
    meta = build_meta('../yfcc100m_dataset')
    #first = list(meta.keys())[0]
    print(meta[first])
