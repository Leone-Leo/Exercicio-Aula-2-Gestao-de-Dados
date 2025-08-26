
CREATE TABLE Vendas (
    ID_Pedido INT,
    Data_Compra DATE,
    SKU_Produto VARCHAR(50),
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Preco_Unitario DECIMAL(10, 2),
    Quantidade INT,
    Custo_Frete DECIMAL(10, 2),
    CEP_Entrega VARCHAR(10),
    Status_Entrega VARCHAR(20)
);
