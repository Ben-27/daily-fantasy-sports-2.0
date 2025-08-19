import torch
import torch.nn as nn
from torch.utils.data import DataLoader
import numpy as np

from datasets import load_tabular_data

MODEL_FACTORY = {}

def calculate_model_size(model: torch.nn.Module) -> float:
    """
    Estimate model size in MBs.
    """
    return sum(p.numel() for p in model.parameters()) * 4 / 1024 / 1024

def load_model(model_name: str, model_path: str, **model_kwargs) -> nn.Module:
    """
    Load a pre-trained model.
    """
    # intialize the model from its class
    m = MODEL_FACTORY[model_name](**model_kwargs)
    # load the saved state dictionary
    m.load_state_dict(torch.load(model_path, weights_only=True, map_location='cpu'))

    print(f'Model size: {calculate_model_size(m):.2f} MB')
    return m

def save_model(model: nn.Module) -> str:
    """
    Save a model.
    """
    model_name = None

    for n, m in MODEL_FACTORY.items():
        if type(model) is m:
            model_name = n

    if model_name is None:
        raise ValueError(f"Model type '{str(type(model))}' not supported")

    output_path = f"saved_models/{model_name}.th"
    torch.save(model.state_dict(), output_path)

    return output_path

class LinearNet(nn.Module):
    def __init__(self, in_dim, num_classes):
        super().__init__()
        # embeddings for alpha features
        self.team_embed = nn.Embedding(32, 3)
        self.opp_embed = nn.Embedding(32, 3)
        self.pos_embed = nn.Embedding(5, 2)
        # network
        self.net = nn.Sequential(
            nn.Linear(in_dim, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU()
        )
        # CORAL classification
        self.classifier = nn.Linear(32, num_classes) # -1)

    def forward(self, x_num, x_cat):
        team = self.team_embed(x_cat[:, 0])
        opp = self.opp_embed(x_cat[:, 1])
        pos = self.pos_embed(x_cat[:, 2])

        x = torch.cat([x_num, team, opp, pos], dim=-1)
        out = self.classifier(self.net(x))
        return out
    
    def predict(self, x_num, x_cat):
        y = self.forward(x_num, x_cat)
        return torch.sum(y > .5)

    
def train(num_epochs=10, lr=.001, batch_size=16):
    if torch.cuda.is_available():
        device = torch.device('cuda')
    else:
        device = 'cpu'

    # load data
    train_dataset, val_dataset, test_dataset = load_tabular_data('modeling/feature_dataset.csv')

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    valid_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

    # initialize model, optimizer, and loss
    model = LinearNet(16, 4)
    model.to(device)

    optim = torch.optim.SGD(model.parameters(), lr=lr)
    loss_func = nn.CrossEntropyLoss()

    global_step = 0
    metrics = {'train_loss': [], 'train_acc': [], 'valid_loss': []}
    for epoch in range(num_epochs):
        
        model.train()
        for x_num, x_cat, label in train_loader:
            x_num = x_num.to(device)
            x_cat = x_cat.to(device)
            label = label.to(device) - 1 # reindex from [1, N] -> [0, N-1]

            pred = model.forward(x_num, x_cat)
            loss_val = loss_func(pred, label)

            optim.zero_grad()
            loss_val.backward()
            optim.step()

            metrics['train_loss'].append(loss_val.item())
            global_step += 1

        with torch.inference_mode():
            model.eval()

            for x_num, x_cat, label in valid_loader:
                x_num = x_num.to(device)
                x_cat = x_cat.to(device)
                label = label.to(device) - 1 # reindex from [1, N] -> [0, N-1]

                pred = model.forward(x_num, x_cat)
                loss_val = loss_func(pred, label)

                metrics['valid_loss'].append(loss_val.item())


        print(
            f'Epoch {epoch+1:2d} / {num_epochs:2d} '
            f'train_loss={np.mean(metrics['train_loss']):.3f} '
            f'valid_loss={np.mean(metrics['valid_loss']):.3f}'
        )


if __name__ == '__main__':
    train()