CREATE TABLE IF NOT EXISTS shopping_lists (
    id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    list_id TEXT,
    name TEXT,
    current_qtd INTEGER,
    target_qtd INTEGER,
    acquired_flag INTEGER,
    FOREIGN KEY(list_id) REFERENCES shopping_lists(id)
);
