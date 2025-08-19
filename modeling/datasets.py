import torch
from torch.utils.data import Dataset, DataLoader
import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder


class DFSDataset(Dataset):
    def __init__(self, numerical_feats, categorical_feats, labels, classification=True):
        self.numerical_feats = torch.tensor(numerical_feats, dtype=torch.float32)
        self.categorical_feats = torch.tensor(categorical_feats, dtype=torch.long)
        self.labels = torch.tensor(labels, dtype=(torch.long if classification else torch.float32)).squeeze()

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        return self.numerical_feats[idx], self.categorical_feats[idx], self.labels[idx]


def load_tabular_data(csv_file):
    # load file
    df = pd.read_csv(csv_file)

    # light data cleaning: replace rank NaN with "average"
    df.loc[df['opp_rank'].isna(), 'opp_rank'] = 16
    df.loc[df['opp_pos_rank'].isna(), 'opp_pos_rank'] = 16

    # separate features and labels
    numerical_cols = ['season', 'week', 'opp_rank', 'opp_pos_rank', 
                      'salary', 'fpts_proj', 'rank_proj', 'tier_proj']
    categorical_cols = ['team', 'opp', 'pos']
    y_col = ['tier_actual']

    # replace categories with indices
    for col in categorical_cols:
        col_map = dict([(k, v) for v, k in enumerate(sorted(df[col].unique()))])
        df[col] = df[col].map(col_map)

    X = df.loc[:, numerical_cols].values
    X_cat = df.loc[:, categorical_cols].values
    y = df.loc[:, y_col].values

    # train/val/test splits
    train_mask = X[:, 0] < 2024
    X_train, X_cat_train, y_train = X[train_mask], X_cat[train_mask], y[train_mask]

    val_mask = (X[:, 0] == 2024) & (X[:, 1] <= 6)
    X_val, X_cat_val, y_val = X[val_mask], X_cat[val_mask], y[val_mask]

    test_mask = (X[:, 0] == 2024) & (X[:, 1] > 6)
    X_test, X_cat_test, y_test = X[test_mask], X_cat[test_mask], y[test_mask]

    # normalize features using training stats
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_val = scaler.transform(X_val)
    X_test = scaler.transform(X_test)

    # wrap into Dataset objects
    train_dataset = DFSDataset(X_train, X_cat_train, y_train)
    val_dataset = DFSDataset(X_val, X_cat_val, y_val)
    test_dataset = DFSDataset(X_test, X_cat_test, y_test)

    return train_dataset, val_dataset, test_dataset


# Example usage
if __name__ == "__main__":
    train_dataset, val_dataset, test_dataset = load_tabular_data('modeling/feature_dataset.csv')

    train_loader = DataLoader(train_dataset, batch_size=4, shuffle=True)
    val_load = DataLoader(val_dataset, batch_size=4, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=4, shuffle=False)

    # Show one batch from train loader
    for numers, cats, labels in train_loader:
        print("Numeric features batch:\n", numers)
        print("Categorical features batch:\n", cats)
        print("Labels batch:\n", labels)
        break
