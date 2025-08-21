import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter
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

class OrdinalCrossEntropyLoss(nn.Module):
    """
    Custom loss function for ordinal classification.
    """
    def __init__(self, class_counts: list):
        super().__init__()
        N = sum(class_counts)
        K = len(class_counts)
        self.class_weights = torch.tensor([N/(K*c) for c in class_counts], dtype=torch.float32)

    def forward(self, logits: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        """
        logits: [B, num_classes-1] raw outputs from model
        targets: [B] class labels in [1, num_classes]
        """
        B, n = logits.shape
        # build binary target matrix
        target_matrix = torch.zeros(B, n, device=logits.device)
        for i in range(n):
            target_matrix[:, i] = (target > i).float()
        
        # get per-sample weights from class
        sample_weights = self.class_weights[target-1].to(logits.device)

        # apply sigmoid to logits and compute weighted log probabilities
        log_probs = nn.functional.logsigmoid(logits) * target_matrix + \
                        (nn.functional.logsigmoid(-logits) * (1 - target_matrix))
        weighted_log_probs = log_probs * sample_weights.unsqueeze(1)

        # negative log likelihood
        loss = -weighted_log_probs.mean()
        return loss

class LinearNet(nn.Module):
    def __init__(self, num_classes: int, input_dim: int, hidden_dims: list[int] = [64, 32]):
        super().__init__()
        # embeddings for alpha features
        self.team_embed = nn.Embedding(32, 3)
        self.opp_embed = nn.Embedding(32, 3)
        self.pos_embed = nn.Embedding(5, 2)
        # network
        c = hidden_dims[0]
        layers = [nn.Linear(input_dim, c), nn.ReLU()]
        for h in hidden_dims:
            layers.append(nn.Linear(c, h))
            layers.append(nn.ReLU())
            c = h
        self.net = nn.Sequential(*layers)
        # CORAL classification
        self.classifier = nn.Linear(h, num_classes-1)

    def forward(self, x_num, x_cat):
        team = self.team_embed(x_cat[:, 0])
        opp = self.opp_embed(x_cat[:, 1])
        pos = self.pos_embed(x_cat[:, 2])

        x = torch.cat([x_num, team, opp, pos], dim=-1)
        out = self.classifier(self.net(x))
        return out
    
    def predict(self, x_num, x_cat):
        logits = self.forward(x_num, x_cat)
        probs = torch.sigmoid(logits)
        return torch.sum(probs > .5, dim=-1) + 1 # convert indices to classes [0, num_classes-1] to [1, num_classes]

    
def train(num_epochs=30, lr=.0005, batch_size=16):
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
    model = LinearNet(4, 16, [128, 32])
    model.to(device)

    # optim = torch.optim.SGD(model.parameters(), lr=lr)
    optim = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)

    # account for class imbalance
    class_sample_count = [2821, 2790, 2813, 50356]
    loss_func = OrdinalCrossEntropyLoss(class_sample_count)

    # initiliaze tensorboard and metrics
    writer = SummaryWriter()
    writer.add_graph(model, [torch.zeros(1, 8, dtype=torch.float), torch.zeros(1, 3, dtype=torch.long)])


    global_step = 0
    metrics = {'train_loss': [], 'train_acc': [], 'valid_acc': []}
    for epoch in range(num_epochs):
        metrics['train_loss'].clear()
        metrics['train_acc'].clear()
        metrics['valid_acc'].clear()

        model.train()
        for x_num, x_cat, label in train_loader:
            x_num = x_num.to(device)
            x_cat = x_cat.to(device)
            label = label.to(device)

            logits = model.forward(x_num, x_cat)
            loss_val = loss_func(logits, label)

            optim.zero_grad()
            loss_val.backward()
            optim.step()

            # log metrics
            preds = model.predict(x_num, x_cat)
            metrics['train_acc'].append((preds == label).float().mean().item())
            metrics['train_loss'].append(loss_val.item())
            global_step += 1
        
        writer.add_scalar('train/loss', np.mean(metrics['train_loss']), epoch+1)
        writer.add_scalar('train/acc', np.mean(metrics['train_acc']), epoch+1)

        with torch.inference_mode():
            model.eval()

            for x_num, x_cat, label in valid_loader:
                x_num = x_num.to(device)
                x_cat = x_cat.to(device)
                label = label.to(device)

                preds = model.predict(x_num, x_cat)

                acc = (preds == label).float().mean().item()
                metrics['valid_acc'].append(acc)

        writer.add_scalar('valid/acc', np.mean(metrics['valid_acc']), epoch+1)

        writer.flush()


        print(
            f'Epoch {epoch+1:2d} / {num_epochs:2d} '
            f'train_loss={np.mean(metrics['train_loss']):.3f} '
            f'train_acc={np.mean(metrics['train_acc']):.3f} '
            f'valid_acc={np.mean(metrics['valid_acc']):.3f}'
        )


if __name__ == '__main__':
    train()