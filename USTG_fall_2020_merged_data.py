import pandas as pd


def data():
    w7_experiment = pd.read_csv('data/anon_w7_experiment.csv')
    w7_tip = pd.read_csv('data/w7_tip_survey.csv')
    w7_tracking = pd.read_csv('data/w7_tracking_survey.csv')
    w11_experiment = pd.read_csv('data/anon_w11_experiment.csv')
    w11_tip = pd.read_csv('data/w11_tip_survey.csv')
    w11_tracking = pd.read_csv('data/w11_tracking_survey.csv')
    return w7_experiment, w7_tip, w7_tracking, w11_experiment, w11_tip, \
        w11_tracking


if __name__ == "__main__":
    # import files
    w7_experiment, w7_tip, w7_tracking, w11_experiment, w11_tip, \
        w11_tracking = data()

    # add prefix w7/w11, experiment, tracking, tip
    # remove excess columns

    # w7_experiment
    col1 = w7_experiment.columns[~w7_experiment.columns.isin(['hashed_id',
                                                              'userid'])]
    w7_experiment.rename(columns=dict(zip(col1, 'w7_experiment' + col1)),
                         inplace=True)
    print(w7_experiment.columns)

    # w7_tracking
    col2 = w7_tracking.columns[~w7_tracking.columns.isin(['hashed_id', 'userid']
                                                         )]
    w7_tracking.rename(columns=dict(zip(col2, 'w7_tracking' + col2)),
                       inplace=True)
    print(w7_tracking.columns)

    # w7_tip
    col3 = w7_tip.columns[~w7_tip.columns.isin(['hashed_id', 'userid']
                                               )]
    w7_tip.rename(columns=dict(zip(col3, 'w7_tip' + col3)),
                  inplace=True)
    print(w7_tip.columns)

    # w11_experiment
    col4 = w11_experiment.columns[~w11_experiment.columns.isin(['hashed_id',
                                                                'userid'])]
    w11_experiment.rename(columns=dict(zip(col4, 'w11_experiment' + col4)),
                          inplace=True)
    print(w11_experiment.columns)

    # w11_tracking
    col4 = w7_tracking.columns[~w11_tracking.columns.isin(['hashed_id', 'userid']
                                                          )]
    w11_tracking.rename(columns=dict(zip(col4, 'w11_tracking' + col4)),
                        inplace=True)
    print(w11_tracking.columns)

    # w11_tip
    col5 = w11_tip.columns[~w11_tip.columns.isin(['hashed_id', 'userid']
                                                 )]
    w11_tip.rename(columns=dict(zip(col5, 'w11_tip' + col5)),
                   inplace=True)
    print(w11_tip.columns)

    # Merging
    # Merge w7
    w7 = pd.merge(w7_experiment, w7_tracking, how='left', left_on=['userid'],
                  right_on=['w7_trackingqreg'])
    w7 = pd.merge(w7, w7_tip, how='left', left_on=['userid'],
                  right_on=['w7_tipqreg'])
    print(w7.columns)

    # Merge w11
    w11 = pd.merge(w11_experiment, w11_tracking, how='left', left_on=['userid'],
                   right_on=['w11_trackingqreg'])
    w11 = pd.merge(w11, w11_tip, how='left', left_on=['userid'],
                   right_on=['w11_tipqreg'])
    print(w11.columns)

    # Merge w7 and w11
    merged = pd.merge(w7, w11, how='outer', on=['hashed_id', 'user_id'])
    print(merged.columns)

    # Delete redundant userid columns
    merged.drop(['w7_tracking_qreg', 'w7_tip_qreg', 'w11_tracking_qreg'
                 'w11_tip_qreg'], axis=1, inplace=True)

    # Delete duplicated userid columns: leave only the 1st index of the same
    # userid for the highest word count, or leave only the 1st index if nothing
    # is written

    # return a dataframe of all duplicated items on userid
    dup = pd.concat(g for _, g in merged.groupby('userid') if len(g) > 1)
    dup = dup.fillna('')

    # create dict s.t. key = userid and values = duplicated index
    # ex. {sg_user0001: 0001, 0002, 0003}

    dup_dict = {}
    index = []
    for i, row in dup.iterrows():
        if row['userid'] not in dup_dict:
            dup_dict[row['userid']] = [i]
        else:
            dup_dict[row['userid']].append(i)

    # work count for each index
    # ex. [[w7#1, w7#2], [w11#1, w11#2]] -> [[0, 20], [0,0]]
    for userid in dup_dict:
        is_null = True
        length = [[], []]
        for dup_index in dup_dict[userid]:
            length[0].append(len(dup.loc[dup_index, 'w7_tip_04']))
            length[1].append(len(dup.loc[dup_index, 'w11_tip_01']))
        print(length)

    # leave first index for userid if all input from the same user
    # userid the same
    if length[0].count(max(length[0])) == length[1].count(max(length[1])) == 0:
        index.append(dup_dict[userid][1:])
        print(f'drop_0 {dup_dict[userid][1:]}')

    # if w7_tip_04 empty, then take index of first max word in w11_tip_01
    elif max(length[0]) == 0:
        dup_dict[userid].pop(length[1].index(max(length[1])))
        index.append((dup_dict[userid]))
        print(f'drop_1 {dup_dict[userid]}')

    # if w11_tip_01 are all empty, take index of first max word in w7_tip_04
    elif max(length[1]) == 0:
        dup_dict[userid].pop(length[0].index(max(length[0])))
        index.append(dup_dict[userid])
        print(f'drop_2 {dup_dict[userid]}')

    # if some work is left in both weeks, choose the most words
    else:
        max_word = 0
        max_index = 0
        for i in range(len(length[0])):
            curr_word = length[0][i] + length[1][i]
            if curr_word > max_word:
                max_word = curr_word
                max_index = i
        dup_dict[userid].pop(max_index)
        print(f'drop_3 {dup_dict[userid]}')
        index.append(dup_dict[userid])

    # drop the index other than what we've selected
    for item in index:
        merged.drop(index=item, inplace=True)

    merged.to_csv('merged_prototype.csv', index=False)




