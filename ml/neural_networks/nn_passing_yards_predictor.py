import pandas as pd
import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# -------------------------------
# Load environment and connect
# -------------------------------
load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

# -------------------------------
# Read and preprocess data
# -------------------------------
df = pd.read_sql_table("final_modeling_data", engine)

# Filter for QBs only
df = df[df["player_position"] == "QB"]
df = df[df["player_id"].astype(str) != "0"]

# Sort by time
df = df.sort_values(by=["player_id", "season", "week"])

# Extract home team from game_id
df["home_team"] = df["game_id"].apply(lambda x: x.split("_")[-1])

# Player moneyline
df["player_moneyline"] = df.apply(
    lambda row: row["home_moneyline"] if row["posteam"] == row["home_team"] else row["away_moneyline"],
    axis=1
)

# -------------------------------
# Features and target
# -------------------------------
numeric_features = [
    "week", "pass_attempt", "complete_pass", "total_pass_plays", "pass_plays_off", "pass_pct_off",
    "red_zone_pass_pct_off", "deep_pass_pct_off", "avg_air_yards_off", "avg_yac_off",
    "man_coverage_pct_def", "zone_coverage_pct_def", "blitz_rate_def", "pressure_rate_def",
    "spread_line", "total_line", "over_odds", "under_odds",
    "player_moneyline",
    "passing_yards_vs_man_rolling3", "passing_yards_vs_zone_rolling3",
    "pass_touchdown_vs_man_rolling3", "pass_touchdown_vs_zone_rolling3"
]

target = "passing_yards"

# Drop NaNs
df_model = df[numeric_features + [target, "player_id", "defense_team_id", "season"]].dropna()

# Encode player and defense
df_model["player_idx"] = df_model["player_id"].astype("category").cat.codes
df_model["def_idx"] = df_model["defense_team_id"].astype("category").cat.codes
num_players = df_model["player_idx"].nunique()
num_defenses = df_model["def_idx"].nunique()

# -------------------------------
# Train/test split (time-based)
# -------------------------------
train_df = df_model[df_model["season"].isin([2021, 2022, 2023])]
test_df = df_model[df_model["season"] == 2024]

X_train_num = train_df[numeric_features].values.astype(np.float32)
X_test_num = test_df[numeric_features].values.astype(np.float32)

player_train_idx = train_df["player_idx"].values
player_test_idx = test_df["player_idx"].values

def_train_idx = train_df["def_idx"].values
def_test_idx = test_df["def_idx"].values

y_train = train_df[target].values.astype(np.float32)
y_test = test_df[target].values.astype(np.float32)

# Scale numeric features
scaler = StandardScaler()
X_train_num = scaler.fit_transform(X_train_num)
X_test_num = scaler.transform(X_test_num)

# -------------------------------
# PyTorch Dataset
# -------------------------------
class QBDataset(Dataset):
    def __init__(self, X_num, player_idx, def_idx, y):
        self.X_num = X_num
        self.player_idx = player_idx
        self.def_idx = def_idx
        self.y = y

    def __len__(self):
        return len(self.y)

    def __getitem__(self, idx):
        return {
            "X_num": torch.tensor(self.X_num[idx], dtype=torch.float32),
            "player_idx": torch.tensor(self.player_idx[idx], dtype=torch.long),
            "def_idx": torch.tensor(self.def_idx[idx], dtype=torch.long),
            "y": torch.tensor(self.y[idx], dtype=torch.float32)
        }

train_dataset = QBDataset(X_train_num, player_train_idx, def_train_idx, y_train)
test_dataset = QBDataset(X_test_num, player_test_idx, def_test_idx, y_test)

train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=64)

# -------------------------------
# Neural network model
# -------------------------------
class QBNN(nn.Module):
    def __init__(self, num_numeric, num_players, num_defenses, emb_size=16, hidden_units=[128, 64]):
        super().__init__()
        self.player_emb = nn.Embedding(num_players, emb_size)
        self.def_emb = nn.Embedding(num_defenses, emb_size)

        input_size = num_numeric + 2 * emb_size
        layers = []
        for units in hidden_units:
            layers.append(nn.Linear(input_size, units))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(0.2))
            input_size = units
        layers.append(nn.Linear(input_size, 1))
        self.fc = nn.Sequential(*layers)

    def forward(self, X_num, player_idx, def_idx):
        player_e = self.player_emb(player_idx)
        def_e = self.def_emb(def_idx)
        X = torch.cat([X_num, player_e, def_e], dim=1)
        out = self.fc(X)
        return out.squeeze()

# -------------------------------
# Training setup
# -------------------------------
device = "cuda" if torch.cuda.is_available() else "cpu"
model = QBNN(num_numeric=X_train_num.shape[1], num_players=num_players, num_defenses=num_defenses).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
criterion = nn.MSELoss()

# -------------------------------
# Training loop
# -------------------------------
epochs = 50
for epoch in range(epochs):
    model.train()
    train_losses = []
    for batch in train_loader:
        optimizer.zero_grad()
        X_num = batch["X_num"].to(device)
        player_idx = batch["player_idx"].to(device)
        def_idx = batch["def_idx"].to(device)
        y = batch["y"].to(device)

        y_pred = model(X_num, player_idx, def_idx)
        loss = criterion(y_pred, y)
        loss.backward()
        optimizer.step()
        train_losses.append(loss.item())

    print(f"Epoch {epoch+1}/{epochs} - Train Loss: {np.mean(train_losses):.4f}")


model.eval()
y_preds = []
with torch.no_grad():
    for batch in test_loader:
        X_num = batch["X_num"].to(device)
        player_idx = batch["player_idx"].to(device)
        def_idx = batch["def_idx"].to(device)
        y_pred = model(X_num, player_idx, def_idx)
        y_preds.extend(y_pred.cpu().numpy())

rmse = np.sqrt(np.mean((y_test - np.array(y_preds))**2))
print(f"Test RMSE: {rmse:.2f}")


pd.DataFrame({
    "player_id": test_df["player_id"],
    "actual_passing_yards": y_test,
    "predicted_passing_yards": y_preds
}).to_csv("../nn/predictions/predicted_nn_qb_passing_yards_2024.csv", index=False)

torch.save(model.state_dict(), "../model_files/qb_nn_passing_yards.pt")
print("✅ Model and predictions saved successfully.")
