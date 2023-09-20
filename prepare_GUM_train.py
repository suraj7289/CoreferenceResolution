import os
import re
import pandas as pd
from collections import defaultdict
import json
import warnings

warnings.filterwarnings("ignore")


def get_span_position(df, sent_id, lookup_entity):
    try:
        filter_df = df[
            (df['sent_token'].str.split('-').str[0] == sent_id) & (df['entity_tag'].str.contains(lookup_entity))]
        span = " ".join(filter_df['tokentext'])
        span_position = filter_df.iloc[0]['position'].split('-')[0] + '-' + \
                        filter_df.iloc[-1]['position'].split('-')[1]
        span_pos = span_position.split('-')
        span_pos = [eval(i) for i in span_pos]
    except:
        span, span_pos = "_", "_"
        print('Unable to get Span Position')
    return span, span_pos


class PrepareCorefTrain():
    cluster_chain_dict1,cluster_chain_dict2 = defaultdict(), defaultdict()

    def load_file_to_df(self,filename,unprocessed_file):
        try:
            data = pd.read_csv(filename, sep='\t', header=None, quoting=3)
            coref_data = data[[0, 1, 2, 3, 9]]
            coref_data.columns = ['sent_token', 'position', 'tokentext', 'entity_tag', 'coref_tag']
        except Exception as e:
            print(f"Unable to process File : {os.path.basename(filename)} in directory {filename}")
            print(f"Error in method <load_file_to_lists> : {e}")
            unprocessed_file.append(filename)

        return coref_data,unprocessed_file

    def create_coref_dicts(self,coref_data):
        cluster_chain_dict1, cluster_chain_dict2 = defaultdict(), defaultdict()
        for index, row in coref_data.iterrows():
            coref_tag = row['coref_tag']
            if coref_tag != '_':
                for c_tag in coref_tag.split('|'):
                    lookup_key = row['sent_token'] + '_' + c_tag.split('_')[1][:-1]
                    lookup_value = c_tag.split('[')[0] + '_' + re.search(r'\[(.+?)\]', c_tag).group(1).split('_')[0]
                    cluster_chain_dict1[lookup_key] = lookup_value
                    cluster_chain_dict2[lookup_key] = 'U'

        return cluster_chain_dict1,cluster_chain_dict2

    def create_clusters(self, key, cluster_i, cluster_chain_dict1, cluster_chain_dict2, clusters_dict):
        clusters_dict[cluster_i].append(key)
        cluster_chain_dict2[key] = 'P'
        next_lookup_value = cluster_chain_dict1[key]
        if next_lookup_value in cluster_chain_dict1:
            self.create_clusters(next_lookup_value, cluster_i, cluster_chain_dict1, cluster_chain_dict2, clusters_dict)
        else:
            clusters_dict[cluster_i].append(next_lookup_value)

        return cluster_chain_dict2,clusters_dict

    def get_texts_and_clusters(self,coref_data,clusters_dict):
        text_inputs = coref_data['tokentext']
        texts = " ".join(text_inputs)
        position_clusters = []
        text_clusters = []
        for _, cluster_list in clusters_dict.items():
            span_list, span_pos_list = [], []
            for c in cluster_list:
                # print(c.split('_')[0],c.split('_')[1])
                span, span_pos = get_span_position(coref_data, c.split('_')[0].split('-')[0], c.split('_')[1])
                span_list.append(span)
                span_pos_list.append(span_pos)
            position_clusters.append(span_pos_list)
            text_clusters.append(span_list)

        return texts,position_clusters,text_clusters

    def get_interim_clusters(self,coref_dict1,coref_dict2):
        cluster_i = 0
        clusters_dict = defaultdict(list)
        for key, value in coref_dict1.items():
            if coref_dict2[key] == 'U':
                coref_dict2, clusters_dict = coref_train.create_clusters(key, cluster_i, coref_dict1, coref_dict2,
                                                                         clusters_dict)
                cluster_i += 1

        return clusters_dict

if __name__ == "__main__":
    filespath = input("Pass input directory:")
    output_path = os.path.join(filespath, "result")
    clean_dir = os.path.join(filespath, 'cleaned')
    try:
        os.mkdir(clean_dir)
        os.mkdir(output_path)
    except Exception as e:
        print(f"Unable to create directory. Error : {e}")


    files = os.listdir(filespath)
    for f in files:
        try:
            if os.path.isfile(os.path.join(filespath,f)):
                target_file_name = os.path.join(clean_dir, f[:-4] + '_clean.tsv')
                with open(os.path.join(filespath,f), 'r', encoding="utf-8") as input_file:
                    with open(os.path.join(clean_dir,target_file_name), 'w', encoding="utf-8") as target_file:
                        lines = input_file.readlines()
                        for line in lines:
                            if line[0].isdigit():
                                target_file.writelines(line)
        except:
            pass

    input_texts_list, coref_clusters_list, position_clusters_list = [], [], []
    clean_files = os.listdir(clean_dir)
    unprocessed_file = []
    for cf in clean_files:
        print(f"Processing File - {cf}")
        coref_train = PrepareCorefTrain()
        try:
            coref_data, unprocessed_file = coref_train.load_file_to_df(os.path.join(clean_dir,cf), unprocessed_file)
            coref_dict1, coref_dict2 = coref_train.create_coref_dicts(coref_data)
            # create clusters of sent_id+lookup entity tag
            clusters_dict = coref_train.get_interim_clusters(coref_dict1, coref_dict2)
            # iterate over clusters_dict to get final output
            texts, position_clusters, text_clusters = coref_train.get_texts_and_clusters(coref_data, clusters_dict)

            input_texts_list.append(texts)
            position_clusters_list.append(position_clusters)
            coref_clusters_list.append(text_clusters)

        except:
            print(f'Unable to process File: {cf}')

    with open(os.path.join(output_path,"GUM_train_data.json"), 'w') as f:
        for x, y, z in zip(input_texts_list, position_clusters_list, coref_clusters_list):
            j = {"text": x, "clusters": y, "clusters_strings": z}
            s = json.dumps(j)
            f.write(f'{s}\n')

    with open(os.path.join(output_path,"unprocessed_files.txt") , 'w') as f:
        for uf in unprocessed_file:
            f.write(uf+'\n')
