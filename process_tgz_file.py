import tables
import hdf5_getters as GETTERS
import os
import string
import glob
import sys

def read_target_track_id_list():
    # Read the track ids with top 100 tags
    with open('new_complet_msd_ids.txt', 'r') as fd:
        data = fd.read()
    
    return data.split(',')

def put_target_track_into_buckets(target_track_id_list):
    
    buckets = {}
    
    for upper_letter in string.ascii_uppercase:
         buckets[upper_letter] = list(filter(lambda track_id: track_id[2] == upper_letter, target_track_id_list))
            
    return buckets

target_track_id_list = read_target_track_id_list()
print(len(target_track_id_list))

target_track_id_list = sorted(target_track_id_list)

target_tracks_buckets = put_target_track_into_buckets(target_track_id_list)
print(len(target_tracks_buckets))

def extract_files_in_bucket(bucket):

    feature_file_handle = open('%s.txt' % (bucket), 'w')

    cnt = 0
    for track_id in target_tracks_buckets[bucket]:
        h5_file_path = track_id[2] + '/' + \
                        track_id[3] + '/' + \
                        track_id[4] + '/' + \
                        track_id + '.h5'

        with tables.open_file('./' + h5_file_path, mode="r") as h5_file_handle:

            mfcc_vec = GETTERS.get_segments_timbre(h5_file_handle).reshape(-1).tolist()
            mfcc_vec.insert(0, track_id)

            to_str_mfcc_vec = list(map(lambda v: str(v), mfcc_vec))
            feature_file_handle.write(','.join(to_str_mfcc_vec) + '\n')

        cnt += 1

        if cnt % 200 == 0:
            print('Processed %d files' % (cnt))

    feature_file_handle.close()
    
    print('Successfully extracted %d files' % (cnt))

fn = sys.argv[1]

print('Working on %s' % (fn))

bucket = fn.split('/')[-1][0]

print('Working on bucket %s' % (bucket))

files_to_extract = target_tracks_buckets[bucket]

print('%d files in total to process' % (len(files_to_extract)))
os.system('tar -zxvf %s -C .' % (fn))
print('Finished extracting tgz file')

extract_files_in_bucket(bucket)

os.system('rm -rf %s' % (bucket))
print('Finished %s' % (fn))