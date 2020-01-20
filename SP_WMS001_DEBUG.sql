USE [TOTVS]
GO

/****** Object:  StoredProcedure [dbo].[SP_WMS001_DEBUG]    Script Date: 09/01/2019 11:03:44 ******/
DROP PROCEDURE [dbo].[SP_WMS001_DEBUG]
GO

/****** Object:  StoredProcedure [dbo].[SP_WMS001_DEBUG]    Script Date: 09/01/2019 11:03:44 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO








CREATE Procedure [dbo].[SP_WMS001_DEBUG] 
    (@p_n_recno_dcf integer,
     @p_n_troca_lote integer = 0,
     @p_c_lote_novo varchar(10),
     @p_c_lote_anterior varchar(10),
     @p_n_peca_media_pequena_min float,
     @p_n_peca_media_pequena_max float,
     @p_n_peca_media_pequena_limite_saldo float,
     @p_n_peca_media_grande_min float,
     @p_n_peca_media_grande_max float,
     @p_n_peca_media_grande_limite_saldo float,
     @p_n_peca_media_limite_saldo_total float,
     @p_n_nuance_qtd_desejada_faixa float,
     @p_n_nuance_saldo_minimo float)
     
  -- DEFINICAO DOS PARAMETROS
  -- p_n_recno_dcf :: R_E_C_N_O_ do registro da tabela DCF para buscar os dados do pedido
  -- p_n_troca_lote :: determina de eh primeira selecao ou teste de troca de lotes
  -- p_c_lote_novo :: determina o codigo do novo lote testado
  -- p_c_lote_anterior :: determina o lote anterior selecionado
  -- p_n_peca_media_pequena_min :: determina a metragem minima para peca media pequena
  -- p_n_peca_media_pequena_max :: determina a metragem maxima para peca media pequena
  -- p_n_peca_media_pequena_limite_saldo :: determina o percentual maximo (limite) do saldo para peca media pequena
  -- p_n_peca_media_grande_min  :: determina a metragem minima para peca media grande
  -- p_n_peca_media_grande_max  :: determina a metragem maxima para peca media grande
  -- p_n_peca_media_grande_limite_saldo :: determina o percentual maximo (limite) do saldo para peca media grande
  -- p_n_peca_media_limite_saldo_total :: determina o percentual maximo (limite) do saldo para peca media pequena + peca media grande
  -- p_n_nuance_qtd_desejada_faixa :: determina a metragem desejada minima para se aplicar a regra de saldo minimo da nuance
  -- p_n_nuance_saldo_minimo :: quando a qtd desejada ultrapassa a qtd desejada acima, aplica a regra de saldo minimo da nuance
  
AS
BEGIN

  -- QSdoBrasil - Set 2018
  -- Rotina para selecao de lotes no estoque para atender o pedido de venda com a melhor combinacao
  -- possivel respeitando as regras dadas;
  --
  -- A rotina busca o saldo disponivel e armazena na tabela t_saldo;
  --
  -- Para cada lote da t_saldo, a rotina cria um novo cenario na tabela t_cenario
  -- A partir do primeiro lote, seleciona mais lotes de acordo com as regras buscando atender o saldo necessario
  --
  -- Havendo um cenario que atenda a todos os requisitos na tabela t_cenario, retorna este cenario para o 
  -- Protheus e empenha os lotes selecionados

  --
  -- TABELA DE CLASSIFICACAO DAS PECAS PELA METRAGEM DO ROLO
  --
  -- PECA PEQUENA :: ENTRE 1.5 E 30 METROS
  -- PECA MEDIA PEQUENA :: ENTRE 31 E 60 METROS
  -- PECA MEDIA GRANDE :: ENTRE 61 E 79 METROS
  -- PECA NORMAL :: A PARTIR DE 80 METROS

  SET NOCOUNT ON
  SET ANSI_WARNINGS OFF

  -- Tabela com as regras provenientes da tabela ZA2 do Protheus
  DECLARE @t_regra
    TABLE (v_c_regra_armazem varchar(2),
           v_c_regra_codigo varchar(4),
           v_c_regra_item varchar(2),
           v_c_regra_tipo varchar(6),                        
           v_c_regra_uf varchar(2),
           v_c_regra_cliente varchar(6),
           v_c_regra_cliente_loja varchar(2),
           v_c_regra_cond_pgto varchar(3),
           v_n_regra_de float,
           v_n_regra_ate float,
           v_n_regra_valor float)

  -- Tabela alimentada com os lotes disponiveis em estoque
  -- que atendam os parametros de peso e largura
  DECLARE @t_saldo
    TABLE (n_recno_d14 int index t_saldo_ix1 clustered,
           c_lote VARCHAR(10),
           c_endereco VARCHAR(15),
           n_peso FLOAT,
           n_largura FLOAT,
           n_saldo FLOAT,
           c_nuance VARCHAR(3),
           c_tipo_peca VARCHAR(30) index t_saldo_ix3,
           n_lote_ja_selecionado INT index t_saldo_ix2,
           n_nuance_igual_pai INT,
           n_endereco_igual_pai INT,
           n_largura_ok INT,
           n_largura_pai_diff FLOAT,
           n_troca_lote_pai INT)

  -- saldo disponivel sintetizado por nuance para avaliar as nuances com mais probabilidade de atender ao cenario
  DECLARE @t_saldo_nuance
    TABLE (c_nuance VARCHAR(3) index t_saldo_nuance_ix1,
           n_saldo_total FLOAT,
           n_saldo_peca_media_pequena FLOAT,
           n_saldo_peca_media_grande FLOAT,
           n_saldo_peca_normal FLOAT,
           n_endereco_diferente FLOAT,
           n_endereco_diferente_fator FLOAT,
           n_saldo_atende_qtd_desejada FLOAT,
           n_saldo_atende_qtd_min FLOAT,
           n_saldo_possui_peca_media FLOAT,
           n_ordem_tentativa FLOAT,
           c_lote_mais_antigo VARCHAR(10))

  -- Tabela alimentada com os cenarios 
  DECLARE @t_cenario
    TABLE (n_cenario INT,
           n_iteracao INT,
           n_ordem INT,
           n_recno_d14 INT,
           c_lote VARCHAR(10),
           c_endereco VARCHAR(15),
           c_nuance VARCHAR(3),
           c_tipo_peca VARCHAR(30),
           n_largura FLOAT,
           n_saldo FLOAT,
           n_saldo_total FLOAT,
           n_saldo_nuance_total FLOAT,
           n_qtd_peca_media_total FLOAT,
           n_qtd_nuance_total FLOAT,      
           n_endereco_diferente FLOAT,     
           n_largura_min FLOAT,
           n_largura_max FLOAT,
           n_largura_variacao FLOAT,
           INDEX t_cenario_ix1 nonclustered (n_cenario, n_recno_d14),
           INDEX t_cenario_ix2 nonclustered (n_cenario, n_iteracao))

   -- Tabela utilizada para selecionar o melhor cenario gerado
   DECLARE @t_cenario_sintetico
     TABLE (n_cenario INT,
            n_qtd_peca_media_total FLOAT,
            n_qtd_peca_media_total_fator FLOAT,
            n_qtd_nuance_total FLOAT, 
            n_qtd_nuance_total_fator FLOAT, 
            n_endereco_diferente FLOAT,
            n_endereco_diferente_fator FLOAT,
            n_qtd_desejada_diff FLOAT,
            n_qtd_desejada_diff_fator FLOAT,
            n_ordem_tentativa FLOAT)
  
  -- tabela utilizada para armazenar as nuances ja utilizadas no cenario e determinar
  -- se podera ou nao selecionar nuance diferente
  -- tabela utilizada para melhoria do desempenho
  DECLARE @t_nuance
    TABLE (n_cenario INT,
           c_nuance VARCHAR(3),
           n_saldo_total_disponivel FLOAT,
           n_saldo_total_limite FLOAT,
           n_saldo_total_selecionado FLOAT,
           n_saldo_peca_media_pequena_limite FLOAT,
           n_saldo_peca_media_pequena_selecionado FLOAT,
           n_saldo_peca_media_grande_limite FLOAT,
           n_saldo_peca_media_grande_selecionado FLOAT,
           index t_nuance_ix1 nonclustered (n_cenario, c_nuance))

  -- Filial DCF
  DECLARE @v_c_filial_dcf varchar(2)
  -- Filial DC3
  DECLARE @v_c_filial_dc3 varchar(2)
  -- Filial D14 :: Saldo
  DECLARE @v_c_filial_d14 varchar(2)
  -- Filial DC8 :: Estrutura Fisica
  DECLARE @v_c_filial_dc8 varchar(2)
  -- Filial ZA2 :: Tabela com as regras
  DECLARE @v_c_filial_za2 varchar(2)
  -- Filial SA1 :: Cliente
  DECLARE @v_c_filial_sa1 varchar(2)
  -- Filial SC5 :: Pedido
  DECLARE @v_c_filial_sc5 varchar(2)
  -- UF do cliente
  DECLARE @v_c_cliente_uf VarChar(2)  
  -- Atributos da regra
  DECLARE @v_c_regra_codigo varchar(4)
 
  -- Regra para limitar quantidade de nuances diferentes que pode ser enviada
  DECLARE @v_n_regra_nuance_max float
  
  -- Regra pela peca media
  DECLARE @v_n_regra_peca_media_qtd_max float  
  DECLARE @v_n_regra_peca_media_pequena_saldo_max float
  DECLARE @v_n_regra_peca_media_grande_saldo_max float
  DECLARE @v_n_regra_peca_media_saldo_max float

  -- Variacao na quantidade total do pedido
  DECLARE @v_n_regra_pedido_variacao_de FLOAT
  DECLARE @v_n_regra_pedido_variacao_ate FLOAT
  DECLARE @v_n_regra_pedido_total_min FLOAT  
  DECLARE @v_n_regra_pedido_total_max FLOAT  
  
  -- Regra para a metragem do rolo
  DECLARE @v_n_regra_rolo_metro_de float
  DECLARE @v_n_regra_rolo_metro_ate float

  -- Regra para a largura do rolo
  DECLARE @v_n_regra_largura_de float
  DECLARE @v_n_regra_largura_ate float
  DECLARE @v_n_regra_largura_variacao FLOAT  
  DECLARE @v_n_largura_min FLOAT
  DECLARE @v_n_largura_max FLOAT
  
  -- Regra pelo peso do rolo
  DECLARE @v_n_regra_rolo_peso_de float
  DECLARE @v_n_regra_rolo_peso_ate float

  -- Regra pela soma do saldo disponivel da nuance
  DECLARE @v_n_saldo_minimo_nuance FLOAT

   -- Atributos da tabela DCF provenientes do pedido
  DECLARE @v_c_armazem varchar(2)
  DECLARE @v_c_produto varchar(15)
  DECLARE @v_c_cliente_codigo varchar(6)
  DECLARE @v_c_cliente_loja varchar(2)
  DECLARE @v_c_documento varchar(6)
  DECLARE @v_c_cond_pgto varchar(3)
  DECLARE @v_n_qtd_desejada float
  DECLARE @v_c_dcf_id varchar(6)

  -- Variaveis do cursor principal
  DECLARE @v_c_pai_lote VARCHAR(10)
  DECLARE @v_c_pai_endereco VARCHAR(15)
  DECLARE @v_n_pai_recno INT
  DECLARE @v_n_pai_peso FLOAT
  DECLARE @v_n_pai_largura FLOAT
  DECLARE @v_n_pai_saldo FLOAT
  DECLARE @v_c_pai_nuance VARCHAR(3)
  DECLARE @v_c_pai_tipo_peca VARCHAR(30)

  -- Variaveis para controle do cenario e ordem
  DECLARE @v_n_cenario INT = 1
  DECLARE @v_n_iteracao INT = 1
  DECLARE @v_n_ordem INT = 1
  DECLARE @v_n_saldo_total FLOAT
  DECLARE @v_n_saldo_peca_normal_total FLOAT
  DECLARE @v_n_saldo_peca_media_pequena_total FLOAT
  DECLARE @v_n_saldo_peca_media_grande_total FLOAT
  DECLARE @v_n_saldo_peca_media_total FLOAT
  
  -- Variaveis para controle do limite de saldo da nuance
  DECLARE @v_n_saldo_nuance_total FLOAT
  DECLARE @v_n_saldo_nuance_limite FLOAT
  DECLARE @v_n_saldo_peca_media_pequena_limite FLOAT
  DECLARE @v_n_saldo_peca_media_grande_limite FLOAT

  DECLARE @v_n_qtd_nuance_total FLOAT
  DECLARE @v_n_qtd_peca_media_total FLOAT
  DECLARE @v_n_endereco_diferente FLOAT

  -- Variaveis para calculo do fator de preferencia de lotes
  DECLARE @v_n_endereco_diferente_min FLOAT
  DECLARE @v_n_endereco_diferente_max FLOAT
  DECLARE @v_n_qtd_nuance_total_min FLOAT
  DECLARE @v_n_qtd_nuance_total_max FLOAT
  DECLARE @v_n_qtd_peca_media_total_min FLOAT
  DECLARE @v_n_qtd_peca_media_total_max FLOAT
  DECLARE @v_n_qtd_desejada_diff_min FLOAT
  DECLARE @v_n_qtd_desejada_diff_max FLOAT

  -- Variaveis para o cursor secundario (lote filho)
  DECLARE @v_c_filho_lote VARCHAR(10)
  DECLARE @v_c_filho_endereco VARCHAR(15)
  DECLARE @v_n_filho_recno INT
  DECLARE @v_n_filho_saldo FLOAT
  DECLARE @v_c_filho_nuance VARCHAR(3)
  DECLARE @v_c_filho_tipo_peca VARCHAR(30)
  DECLARE @v_n_filho_largura FLOAT

  -- RECNO do lote novo
  DECLARE @v_n_recno_d14_lote_novo INT

  -- Cenario selecionado
  DECLARE @v_n_cenario_selecionado INT
 
  --
  -- INFORMACOES DO PEDIDO
  --
  BEGIN
    -- Extrai informacoes do pedido desejado
    SELECT @v_c_filial_dcf  = DCF.DCF_FILIAL,
           @v_c_cliente_codigo  = DCF.DCF_CLIFOR, 
           @v_c_cliente_loja  = DCF.DCF_LOJA,
           @v_c_documento     = DCF.DCF_DOCTO,
           @v_n_qtd_desejada   = DCF.DCF_QTDORI,
           @v_c_produto = DCF.DCF_CODPRO,
           @v_c_armazem   = DCF.DCF_LOCAL,
           @v_c_dcf_id   = DCF.DCF_ID
      FROM DCF010 AS DCF WITH (NOLOCK)
     WHERE DCF.R_E_C_N_O_ = @p_n_recno_dcf

    -- Determina o codigo de filial para cada tabela utilizada
    EXEC dbo.XFILIAL_18_01 'DC3', @v_c_filial_dcf , @v_c_filial_dc3 output
    EXEC dbo.XFILIAL_18_01 'D14', @v_c_filial_dcf , @v_c_filial_d14 output
    EXEC dbo.XFILIAL_18_01 'DC8', @v_c_filial_dcf , @v_c_filial_dc8 output
    EXEC dbo.XFILIAL_18_01 'ZA2', @v_c_filial_dcf , @v_c_filial_za2 output
    EXEC dbo.XFILIAL_18_01 'SA1', @v_c_filial_dcf , @v_c_filial_sa1 output
    EXEC dbo.XFILIAL_18_01 'SC5', @v_c_filial_dcf , @v_c_filial_sc5 output
        
    -- UF do Cliente
    SELECT @v_c_cliente_uf = SA1.A1_EST 
      FROM SA1010 AS SA1 WITH (NOLOCK)
     WHERE SA1.A1_FILIAL  = @v_c_filial_sa1 
       AND SA1.A1_COD     = @v_c_cliente_codigo 
       AND SA1.A1_LOJA    = @v_c_cliente_loja
       AND SA1.D_E_L_E_T_ <> '*'
  
    -- Condicao de pagamento do pedido
    SELECT @v_c_cond_pgto = SC5.C5_CONDPAG 
      FROM SC5010 AS SC5 WITH (NOLOCK)
     WHERE SC5.C5_FILIAL  = @v_c_filial_sc5 
       AND SC5.C5_NUM     = @v_c_documento 
       AND SC5.C5_CLIENTE = @v_c_cliente_codigo 
       AND SC5.C5_LOJACLI = @v_c_cliente_loja
       AND SC5.D_E_L_E_T_ <> '*'
  END
  
  --
  -- LEITURA DAS REGRAS
  --        
  BEGIN
    --Determina qual regra da tabela ZA2 sera utilizada
    SELECT TOP 1 @v_c_regra_codigo = ZA2.ZA2_CODIGO 
      FROM ZA2010 ZA2 with (NOLOCK) 
     WHERE ZA2.ZA2_FILIAL = @v_c_filial_za2
       AND ZA2.ZA2_LOCAL = @v_c_armazem
       AND ZA2.D_E_L_E_T_ <> '*'
       AND ((ZA2_CLIENT = @v_c_cliente_codigo AND ZA2_LOJA = @v_c_cliente_loja) OR (ZA2_CLIENT = '      ' AND ZA2_LOJA = '  '))
       AND ((ZA2_ESTADO = @v_c_cliente_uf) OR (ZA2_ESTADO = '  '))
     ORDER BY ZA2_CLIENT DESC, ZA2_LOJA DESC, ZA2_ESTADO DESC

    -- Cursor de Analise da regra escolhida
    INSERT into @t_regra 
          (v_c_regra_armazem, 
           v_c_regra_codigo, 
           v_c_regra_item, 
           v_c_regra_tipo, 
           v_c_regra_uf, 
           v_c_regra_cliente, 
           v_c_regra_cliente_loja, 
           v_c_regra_cond_pgto, 
           v_n_regra_de, 
           v_n_regra_ate, 
           v_n_regra_valor) 
    SELECT '01' AS ZA2_LOCAL, 
           ZA2.ZA2_CODIGO, 
           ZA2.ZA2_ITEM, 
           ZA2.ZA2_REGRA, 
           ZA2.ZA2_ESTADO, 
           ZA2.ZA2_CLIENT, 
           ZA2.ZA2_LOJA, 
           ZA2.ZA2_PAGTO, 
           ZA2.ZA2_DE, 
           ZA2.ZA2_ATE, 
           ZA2.ZA2_VALOR 
      FROM ZA2010 ZA2 with (NOLOCK) 
     WHERE ZA2.ZA2_FILIAL = @v_c_filial_za2 
       AND ZA2.ZA2_CODIGO = @v_c_regra_codigo
       AND ZA2.ZA2_LOCAL = @v_c_armazem
       AND ZA2.D_E_L_E_T_ <> '*'
     ORDER BY ZA2_CODIGO, ZA2_ITEM

    -- Limita a uma regra de MTPED por PAGTO
    IF (SELECT COUNT(1) FROM @t_regra WHERE v_c_regra_cond_pgto = @v_c_cond_pgto AND v_c_regra_tipo = 'MTPED') > 0
       -- Se houver regra especifica para a condicao de pagamento elimina as demais
       DELETE @t_regra 
        WHERE v_c_regra_tipo = 'MTPED' 
          AND v_c_regra_cond_pgto <> @v_c_cond_pgto
    ELSE
      -- Se nao houver regra especifica para a condicao de pagamento utiliza a generica (cond. pgto em branco)
      DELETE @t_regra 
       WHERE v_c_regra_tipo  = 'MTPED' 
         AND v_c_regra_cond_pgto <> '   '

    SELECT @v_n_regra_pedido_variacao_de = v_n_regra_de, @v_n_regra_pedido_variacao_ate = v_n_regra_ate
      FROM @t_regra 
     WHERE v_c_regra_tipo = 'MTPED'
       
    SET @v_n_regra_pedido_total_min = @v_n_qtd_desejada * (1 + (@v_n_regra_pedido_variacao_de / 100))
  
    SET @v_n_regra_pedido_total_max = @v_n_qtd_desejada * (1 + (@v_n_regra_pedido_variacao_ate /100))       
    
    -- Limita a uma regra de LARG por PAGTO
    IF (SELECT COUNT(1) FROM @t_regra WHERE v_c_regra_cond_pgto = @v_c_cond_pgto AND v_c_regra_tipo = 'LARG') > 0
       -- Se houver regra especifica para a condicao de pagamento elimina as demais
       DELETE @t_regra 
        WHERE v_c_regra_tipo = 'LARG' 
          AND v_c_regra_cond_pgto <> @v_c_cond_pgto
    ELSE
      -- Se nao houver regra especifica para a condicao de pagamento utiliza a generica (cond. pgto em branco)
      DELETE @t_regra 
       WHERE v_c_regra_tipo  = 'LARG' 
         AND v_c_regra_cond_pgto <> '   '

    -- Verifica se eh necessario aplicar regra por largura do rolo
    SELECT TOP 1 @v_n_regra_largura_de = v_n_regra_de, @v_n_regra_largura_ate = v_n_regra_ate 
      FROM @t_regra 
     WHERE v_c_regra_tipo = 'LARG' 
     
    IF @v_n_regra_largura_de IS NOT NULL AND @v_n_regra_largura_ate IS NOT NULL
     SET @v_n_regra_largura_variacao = @v_n_regra_largura_ate - @v_n_regra_largura_de
  
    -- Verifica se eh necessario aplicar regra pela metragem do rolo
    SELECT TOP 1 @v_n_regra_rolo_metro_de = v_n_regra_de, @v_n_regra_rolo_metro_ate = v_n_regra_ate 
      FROM @t_regra 
      WHERE v_c_regra_tipo = 'MTROLO' 
    
    -- Verifica se eh necessario aplicar regra pelo peso do rolo
    SELECT TOP 1 @v_n_regra_rolo_peso_de = v_n_regra_de, @v_n_regra_rolo_peso_ate = v_n_regra_ate
     FROM @t_regra 
    WHERE v_c_regra_tipo = 'PSROLO' 
  
    -- Verifica se eh necessario aplicar regra nuance
    SELECT TOP 1 @v_n_regra_nuance_max = v_n_regra_valor 
      FROM @t_regra 
     WHERE v_c_regra_tipo = 'NUANCE' 
       AND (@v_n_qtd_desejada >= v_n_regra_de AND @v_n_qtd_desejada <= v_n_regra_ate)

    -- Verifica se eh necessario aplicar regra com maximo de pecas medias
    -- Existem clientes que nao permitem peca media
    SELECT TOP 1 @v_n_regra_peca_media_qtd_max = v_n_regra_valor 
      FROM @t_regra
     WHERE v_c_regra_tipo = 'PCMED'
       AND (@v_n_qtd_desejada >= v_n_regra_de AND @v_n_qtd_desejada <= v_n_regra_ate)

    IF ISNULL(@v_n_regra_peca_media_qtd_max, - 1) <> 0
    BEGIN
      SET @v_n_regra_peca_media_pequena_saldo_max = @v_n_qtd_desejada * @p_n_peca_media_pequena_limite_saldo
      SET @v_n_regra_peca_media_grande_saldo_max = @v_n_qtd_desejada * @p_n_peca_media_grande_limite_saldo
      SET @v_n_regra_peca_media_saldo_max = @v_n_qtd_desejada * @p_n_peca_media_limite_saldo_total
    END
    ELSE
    BEGIN
      SET @v_n_regra_peca_media_pequena_saldo_max = 0
      SET @v_n_regra_peca_media_grande_saldo_max = 0
      SET @v_n_regra_peca_media_saldo_max = 0
    END

    -- Se a quantidade desejada ultrapassa a faixa deve limitar as nuances pela quantidade minima
    IF @v_n_qtd_desejada > @p_n_nuance_qtd_desejada_faixa
       SET @v_n_saldo_minimo_nuance = @p_n_nuance_saldo_minimo
    ELSE
      SET @v_n_saldo_minimo_nuance = 0
  END

  --
  -- CARGA DO SALDO DISPONIVEL
  --
  BEGIN
    -- Se estiver testando um lote existente, o saldo eh o cenario calculado previamente
    If (@p_n_troca_lote = 1)
    BEGIN
      --Garante o lote PAI como primeiro o LOTE NOVO NO t_saldo
      INSERT INTO @t_saldo
            (c_lote,
             c_endereco,
             n_recno_d14,
             c_nuance,
             n_peso,
             n_largura,
             n_saldo,
             n_troca_lote_pai)
      SELECT D14.D14_LOTECT AS c_lote,  
             D14.D14_ENDER AS c_endereco,
             D14.R_E_C_N_O_,
             D14.D14_NUANCE AS c_nuance,          
             D14.D14_PESO AS n_peso,
             D14.D14_LARGUR AS n_largura,
             (D14.D14_QTDEST - (D14.D14_QTDEMP + D14.D14_QTDBLQ + D14.D14_QTDSPR)) AS n_saldo,
             1 AS n_troca_lote_pai
        FROM D14010 D14 with (NOLOCK)
        JOIN SBE010 SBE WITH (NOLOCK)
          ON SBE.BE_FILIAL = D14.D14_FILIAL
         AND SBE.BE_LOCAL = D14.D14_LOCAL
         AND SBE.BE_LOCALIZ = D14.D14_ENDER
         AND SBE.BE_STATUS <> '3' -- Desprezar enderecos bloqueados
         AND SBE.D_E_L_E_T_ <> '*'
       WHERE D14.D14_FILIAL = @v_c_filial_d14 
         AND D14.D14_LOCAL  = @v_c_armazem 
         AND D14.D14_PRODUT = @v_c_produto
         AND D14.D14_LOTECT = @p_c_lote_novo
         AND D14.D_E_L_E_T_ <> '*'
       ORDER BY D14_LOTECT

      SELECT @v_n_recno_d14_lote_novo = MAX(s.n_recno_d14)
        FROM @t_saldo s
       WHERE s.n_troca_lote_pai = 1
 
      --Garante OS D12 PAI sem LOTE VELHO
      INSERT INTO @t_saldo
            (c_lote,
             c_endereco,
             n_recno_d14,
             c_nuance,
             n_peso,
             n_largura,
             n_saldo,
             n_troca_lote_pai)
      SELECT D14.D14_LOTECT AS c_lote,  
             D14.D14_ENDER AS c_endereco,
             D14.R_E_C_N_O_,
             D14.D14_NUANCE AS c_nuance,          
             D14.D14_PESO AS n_peso,
             D14.D14_LARGUR AS n_largura,
             (D14.D14_QTDEST - (D14.D14_QTDEMP + D14.D14_QTDBLQ )) AS n_saldo,
             0 as n_troca_lote_pai
        FROM D14010 D14 with (NOLOCK)
        JOIN SBE010 SBE WITH (NOLOCK)
          ON SBE.BE_FILIAL = D14.D14_FILIAL
         AND SBE.BE_LOCAL = D14.D14_LOCAL
         AND SBE.BE_LOCALIZ = D14.D14_ENDER
         AND SBE.BE_STATUS <> '3' -- Desprezar enderecos bloqueados
         AND SBE.D_E_L_E_T_ <> '*'
       WHERE D14.D14_FILIAL = @v_c_filial_d14 
         AND D14.D14_LOCAL  = @v_c_armazem 
         AND D14.D14_PRODUT = @v_c_produto
         AND D14.D14_LOTECT IN (SELECT D12.D12_LOTECT 
                                  FROM D12010 D12 
                                 WHERE D12.D12_IDDCF = @v_c_dcf_id 
                                   AND D12.D12_LOTECT <> @p_c_lote_anterior 
                                   AND D12.D_E_L_E_T_<>'*')
         AND D14.D_E_L_E_T_ <> '*'
       ORDER BY D14_LOTECT
    END
    ELSE
      -- Busca todo o saldo disponivel
      INSERT INTO @t_saldo
            (c_lote,
             c_endereco,
             n_recno_d14,
             c_nuance,
             n_peso,
             n_largura,
             n_saldo,
             n_troca_lote_pai)
      SELECT D14.D14_LOTECT AS c_lote,  
             D14.D14_ENDER AS c_endereco,
             D14.R_E_C_N_O_,
             D14.D14_NUANCE AS c_nuance,          
             D14.D14_PESO AS n_peso,
             D14.D14_LARGUR AS n_largura,
             (D14.D14_QTDEST - (D14.D14_QTDEMP + D14.D14_QTDBLQ + D14.D14_QTDSPR)) AS n_saldo,
             0 AS n_troca_lote_pai
        FROM D14010 D14 with (NOLOCK)
        JOIN DC3010 DC3 with (NOLOCK)
          ON DC3.DC3_FILIAL = '  ' 
         AND DC3.DC3_LOCAL  = D14.D14_LOCAL 
         AND DC3.DC3_CODPRO = D14.D14_PRODUT 
         AND DC3.DC3_TPESTR = D14.D14_ESTFIS 
         AND DC3.D_E_L_E_T_ <> '*'
        JOIN DC8010 DC8 with (NOLOCK)
          ON DC8.DC8_FILIAL = '  ' 
         AND DC8.DC8_CODEST = D14.D14_ESTFIS
         AND DC8.DC8_TPESTR IN ('1','2','3','4','6')
         AND DC8.D_E_L_E_T_ <> '*'
        JOIN SBE010 SBE WITH (NOLOCK)
          ON SBE.BE_FILIAL = D14.D14_FILIAL
         AND SBE.BE_LOCAL = D14.D14_LOCAL
         AND SBE.BE_LOCALIZ = D14.D14_ENDER
         AND SBE.BE_STATUS <> '3' -- Desprezar enderecos bloqueados
         AND SBE.D_E_L_E_T_ <> '*'
       WHERE D14.D14_FILIAL = @v_c_filial_d14 
         AND D14.D14_LOCAL  = @v_c_armazem 
         AND D14.D14_PRODUT = @v_c_produto
         AND D14.D14_ETIQUE <> ' ' 
         AND (D14_QTDEST-(D14_QTDEMP+D14_QTDBLQ+D14_QTDSPR)) > 0
         AND D14.D14_QTDSPR = 0
         -- Restricao por metragem do rolo (o saldo eh a metragem)
         AND (@v_n_regra_rolo_metro_de IS NULL OR (D14_QTDEST-(D14_QTDEMP+D14_QTDBLQ+D14_QTDSPR)) >= @v_n_regra_rolo_metro_de) 
         AND (@v_n_regra_rolo_metro_ate IS NULL OR (D14_QTDEST-(D14_QTDEMP+D14_QTDBLQ+D14_QTDSPR)) <= @v_n_regra_rolo_metro_ate) 
         -- Restricao por peso do rolo
         AND (@v_n_regra_rolo_peso_de IS NULL OR D14.D14_PESO >= @v_n_regra_rolo_peso_de)
         AND (@v_n_regra_rolo_peso_ate IS NULL OR D14.D14_PESO <= @v_n_regra_rolo_peso_ate)
         AND D14.D_E_L_E_T_ <> '*'
       ORDER BY D14_LOTECT
    END

    BEGIN
      -- Determina se eh peca media e as larguras compativeis
      UPDATE @t_saldo
         SET c_tipo_peca = CASE 
                             WHEN n_saldo < @p_n_peca_media_pequena_min THEN
                               'PECA PEQUENA'
                             WHEN n_saldo BETWEEN @p_n_peca_media_pequena_min AND @p_n_peca_media_pequena_max THEN
                               'PECA MEDIA PEQUENA'
                             WHEN n_saldo BETWEEN @p_n_peca_media_grande_min AND @p_n_peca_media_grande_max THEN
                               'PECA MEDIA GRANDE'
                             ELSE
                               'PECA NORMAL'
                           END

      IF @v_n_regra_peca_media_qtd_max = 0
        DELETE @t_saldo
         WHERE c_tipo_peca <> 'PECA NORMAL'
    END

    --
    -- SINTETIZA O SALDO POR NUANCE
    --
    BEGIN
      INSERT INTO @t_saldo_nuance (c_nuance, n_saldo_total, n_saldo_peca_media_pequena, n_saldo_peca_media_grande, n_saldo_peca_normal, c_lote_mais_antigo, n_endereco_diferente)
      SELECT s.c_nuance,
             ISNULL(SUM(s.n_saldo), 0) AS n_saldo_total,
             ISNULL(SUM(CASE WHEN s.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN s.n_saldo END), 0) AS n_saldo_peca_media_pequena,
             ISNULL(SUM(CASE WHEN s.c_tipo_peca = 'PECA MEDIA GRANDE' THEN s.n_saldo END), 0) AS n_saldo_peca_media_grande,
             ISNULL(SUM(CASE WHEN s.c_tipo_peca = 'PECA NORMAL' THEN s.n_saldo END), 0) AS n_saldo_peca_normal,
             CASE
               WHEN @p_n_troca_lote = 1 THEN
               -- Se estiver trocando lote tenta iniciar com o lote novo
               MAX(CASE WHEN s.n_troca_lote_pai = 1 THEN s.c_lote END)
             ELSE
               -- Se for cenario normal tenta com o lote mais antigo
               MIN(s.c_lote)
             END AS c_lote_mais_antigo,
             COUNT(DISTINCT s.c_endereco) AS n_endereco_diferente
        FROM @t_saldo s
       GROUP BY s.c_nuance

      -- Classifica o numero de enderecos diferentes entre 0 e 1 para utilizar na classificacao que determina a ordem de tentativa
      SELECT @v_n_endereco_diferente_min = MIN(s.n_endereco_diferente),
             @v_n_endereco_diferente_max = MAX(s.n_endereco_diferente)
        FROM @t_saldo_nuance s

      UPDATE @t_saldo_nuance 
         SET n_endereco_diferente_fator = 1 - ((n_endereco_diferente - @v_n_endereco_diferente_min) / CASE WHEN @v_n_endereco_diferente_max - @v_n_endereco_diferente_min > 0 THEN @v_n_endereco_diferente_max - @v_n_endereco_diferente_min ELSE 1 END)

      -- Verifica se a nuance pode atender a qtd desejada total do pedido (minimizar numero de nuances)
      UPDATE @t_saldo_nuance
         SET n_saldo_atende_qtd_desejada = CASE WHEN @v_n_qtd_desejada <= (n_saldo_total) THEN 1 ELSE 0 END

      -- Verifica se, de acordo com a quantidade desejada, existe minimo para o saldo da nuance
      UPDATE @t_saldo_nuance
         SET n_saldo_atende_qtd_min = CASE WHEN n_saldo_total >= @v_n_saldo_minimo_nuance THEN 1 ELSE 0 END

      -- Verifica se a nuance possui saldo de peca media
      UPDATE @t_saldo_nuance
         SET n_saldo_possui_peca_media = CASE
                                           WHEN n_saldo_peca_media_pequena > 0 OR n_saldo_peca_media_grande > 0 THEN
                                             1
                                           ELSE 
                                             0
                                         END
    
      -- Se a quantidade desejada eh maior que a faixa minima, exclui do saldo as nuances que nao atendem a quantidade minima
      IF @v_n_qtd_desejada > @p_n_nuance_qtd_desejada_faixa
      BEGIN
        DELETE s
          FROM @t_saldo s
         WHERE EXISTS (SELECT 1 
                         FROM @t_saldo_nuance n
                        WHERE n.c_nuance = s.c_nuance
                          AND n.n_saldo_atende_qtd_min = 0)

        DELETE @t_saldo_nuance
         WHERE n_saldo_atende_qtd_min = 0
      END

      -- Soma os campos para determinar a ordem de tentativa de processar as nuances
      UPDATE @t_saldo_nuance
         SET n_ordem_tentativa = n_saldo_atende_qtd_desejada + n_saldo_possui_peca_media + n_saldo_atende_qtd_min + n_endereco_diferente_fator
    END

  --
  -- MONTAGEM DOS CENARIOS
  --
  -- Agrupa os lotes disponiveis por nuance, largura e saldo
  -- Para cada linha encontrada tenta montar um cenario
  DECLARE C_LOTE
   CURSOR FOR
   SELECT c_pai_lote,
          (SELECT MIN(s1.n_recno_d14) FROM @t_saldo s1 WHERE s1.c_lote = q.c_pai_lote) AS n_pai_recno,
          c_pai_nuance, 
          c_pai_endereco,
          c_pai_tipo_peca,
          n_pai_largura, 
          n_pai_saldo
     FROM (
   SELECT MIN(s.c_lote) AS c_pai_lote,
          s.c_nuance AS c_pai_nuance,
          s.c_endereco AS c_pai_endereco,
          s.c_tipo_peca AS c_pai_tipo_peca,
          s.n_largura AS n_pai_largura,
          s.n_saldo AS n_pai_saldo,
          n.n_ordem_tentativa AS n_ordem_tentativa     
     FROM @t_saldo s
     JOIN @t_saldo_nuance n
       ON n.c_nuance = s.c_nuance
    GROUP BY s.c_nuance,
             s.c_endereco,
             s.c_tipo_peca,
             s.n_largura,
             s.n_saldo,
             n.n_ordem_tentativa) q
    -- n_ordem_tentativa :: nuance com mais chance de atender o pedido
    -- c_pai_lote :: FIFO
    ORDER BY n_ordem_tentativa DESC, q.c_pai_lote

  OPEN C_LOTE

  FETCH NEXT 
   FROM C_LOTE
   INTO @v_c_pai_lote, @v_n_pai_recno, @v_c_pai_nuance, @v_c_pai_endereco, @v_c_pai_tipo_peca, @v_n_pai_largura, @v_n_pai_saldo

  WHILE (@@FETCH_STATUS = 0)
  BEGIN
    --
    -- PRIMEIRA ITERACAO :: SELECAO DO LOTE PAI DO CENARIO
    --
    SET @v_n_ordem = 1
    SET @v_n_iteracao = 1

    INSERT INTO @t_cenario (n_cenario, n_iteracao, n_ordem, c_lote, c_nuance, c_endereco, n_largura, n_saldo, c_tipo_peca, n_recno_d14) 
    VALUES (@v_n_cenario, @v_n_iteracao, @v_n_ordem, @v_c_pai_lote, @v_c_pai_nuance, @v_c_pai_endereco, @v_n_pai_largura, @v_n_pai_saldo, @v_c_pai_tipo_peca, @v_n_pai_recno)

    -- Atualiza variaveis controladoras do saldo total selecionado
    SELECT @v_n_saldo_total = ISNULL(SUM(c.n_saldo), 0),
           @v_n_saldo_peca_normal_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA NORMAL' THEN c.n_saldo END), 0),
           @v_n_saldo_peca_media_pequena_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0),
           @v_n_saldo_peca_media_grande_total  = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0),
           @v_n_qtd_peca_media_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' OR c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN 1 END), 0),
           @v_n_qtd_nuance_total = ISNULL(COUNT(DISTINCT c.c_nuance), 0),
           @v_n_endereco_diferente = ISNULL(COUNT(DISTINCT c.c_endereco), 0),
           @v_n_largura_min = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MAX(n_largura) - @v_n_regra_largura_variacao END,
           @v_n_largura_max = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MIN(n_largura) + @v_n_regra_largura_variacao END
      FROM @t_cenario c
     WHERE c.n_cenario = @v_n_cenario

    --
    -- Atualiza os demais registros na tabela de saldo (recno <> pai_recno)
    -- Atualiza todos os registros para limpar iteracoes anteriores
    --
    UPDATE @t_saldo
       SET n_lote_ja_selecionado = CASE WHEN n_recno_d14 = @v_n_pai_recno THEN 1 ELSE 0 END,
           n_nuance_igual_pai = CASE WHEN c_nuance = @v_c_pai_nuance THEN 1 ELSE 0 END,
           n_endereco_igual_pai = CASE WHEN c_endereco = @v_c_pai_endereco THEN 1 ELSE 0 END,
           n_largura_ok = CASE 
                            WHEN @v_n_regra_largura_variacao IS NOT NULL THEN
                              CASE 
                                WHEN n_largura BETWEEN @v_n_largura_min AND @v_n_largura_max THEN
                                  1
                                ELSE
                                  0
                              END
                            ELSE 1
                          END,
           n_largura_pai_diff  = ABS(n_largura - @v_n_pai_largura)

    INSERT INTO @t_nuance 
               (n_cenario,
                c_nuance, 
                n_saldo_total_disponivel, 
                n_saldo_total_selecionado, 
                n_saldo_total_limite, 
                n_saldo_peca_media_pequena_selecionado,
                n_saldo_peca_media_pequena_limite,
                n_saldo_peca_media_grande_selecionado,
                n_saldo_peca_media_grande_limite) 
    SELECT @v_n_cenario,
           s.c_nuance, 
           SUM(s.n_saldo) AS n_saldo_total_disponivel,
           @v_n_pai_saldo as n_saldo_total_selecionado,
           CASE 
             WHEN SUM(s.n_saldo) > @v_n_qtd_desejada THEN 
             SUM(s.n_saldo)
           ELSE
             (@v_n_qtd_desejada - @v_n_saldo_minimo_nuance)
           END as n_saldo_total_limite,
           SUM(CASE WHEN @v_c_pai_tipo_peca = 'PECA MEDIA PEQUENA' THEN s.n_saldo ELSE 0 END) as n_saldo_peca_media_pequena_selecionado,
           CASE
             WHEN @v_n_regra_nuance_max > 0 THEN          
             @v_n_qtd_desejada * (@p_n_peca_media_pequena_limite_saldo / @v_n_regra_nuance_max) 
           END as n_saldo_peca_media_pequena_limite,
           SUM(CASE WHEN @v_c_pai_tipo_peca = 'PECA MEDIA GRANDE' THEN s.n_saldo ELSE 0 END) as n_saldo_peca_media_grande_selecionado,
           CASE
             WHEN @v_n_regra_nuance_max > 0 THEN
               @v_n_qtd_desejada * (@p_n_peca_media_grande_limite_saldo / @v_n_regra_nuance_max) 
           END as n_saldo_peca_media_grande_limite
      FROM @t_saldo s
     WHERE s.c_nuance = @v_c_pai_nuance
       AND s.n_largura_ok = 1
     GROUP BY s.c_nuance

    SELECT @v_n_saldo_nuance_total = n.n_saldo_total_selecionado,
           @v_n_saldo_nuance_limite = n.n_saldo_total_limite,
           @v_n_saldo_peca_media_pequena_total = n.n_saldo_peca_media_pequena_selecionado,
           @v_n_saldo_peca_media_pequena_limite = n.n_saldo_peca_media_pequena_limite,
           @v_n_saldo_peca_media_grande_total = n.n_saldo_peca_media_grande_selecionado,
           @v_n_saldo_peca_media_grande_limite = n.n_saldo_peca_media_grande_limite
      FROM @t_nuance n
     WHERE n.n_cenario = @v_n_cenario
       AND n.c_nuance = @v_c_pai_nuance

    --
    -- PROXIMA ITERACAO TENTA SELECIONAR O MAXIMO DE LOTES POSSIVEIS DE UMA SO VEZ
    -- Somente com a mesma nuance do pai
    -- Somente no mesmo endereco do pai
    -- Com variacao de tamanho para maior (@v_n_pai_largura + @v_n_regra_largura_variacao)
    --
    IF @v_n_saldo_total <  @v_n_qtd_desejada
   AND @v_n_saldo_nuance_total < @v_n_saldo_nuance_limite
    BEGIN

      SET @v_n_iteracao += 1

      -- Tenta buscar o maximo de linhas possiveis em mais uma iteracao
      INSERT INTO @t_cenario (n_cenario, n_iteracao, n_ordem, c_lote, c_endereco, c_nuance, n_largura, c_tipo_peca, n_saldo, n_recno_d14) 
      SELECT @v_n_cenario,
             @v_n_iteracao,
             n_ordem,
             c_lote, 
             c_endereco,
             c_nuance, 
             n_largura,
             c_tipo_peca,
             n_saldo, 
             n_recno_d14
        FROM (
  
      SELECT @v_n_ordem + ROW_NUMBER() OVER (ORDER BY n_largura_pai_diff, c_lote) as n_ordem,
             -- Soma cumulativa
             SUM(n_saldo) OVER (ORDER BY n_largura_pai_diff, c_lote ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_saldo_total,
             c_lote, 
             c_endereco,
             c_nuance, 
             n_largura,
             c_tipo_peca,
             n_saldo, 
             n_recno_d14           
        FROM (
  
      SELECT s.c_lote, 
             s.c_endereco,
             s.n_endereco_igual_pai,
             s.c_nuance, 
             s.n_largura,
             s.c_tipo_peca,
             s.n_saldo, 
             s.n_recno_d14,
             s.n_largura_pai_diff
        FROM @t_saldo s
           -- lote ainda nao selecionado
       WHERE s.n_lote_ja_selecionado = 0
         AND s.n_nuance_igual_pai = 1
         AND s.n_endereco_igual_pai = 1
         AND s.n_largura_ok = 1
         AND (@v_n_largura_min IS NULL OR s.n_largura >= @v_n_pai_largura)
         AND (@v_n_largura_max IS NULL OR s.n_largura <= @v_n_largura_max)
         AND s.c_tipo_peca = 'PECA NORMAL'             
             ) Q            
             ) Q2
       WHERE Q2.n_saldo_total <= (@v_n_regra_pedido_total_max - @v_n_saldo_total)
         AND (Q2.n_saldo_total + @v_n_saldo_nuance_total) <= @v_n_saldo_nuance_limite     
       ORDER BY n_ordem

      -- se conseguiu pescar um ou mais lotes
      IF @@ROWCOUNT > 0
      BEGIN
      -- Atualiza variaveis controladoras do saldo total selecionado
        SELECT @v_n_saldo_total = ISNULL(SUM(c.n_saldo), 0),
               @v_n_saldo_peca_normal_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA NORMAL' THEN c.n_saldo END), 0),
               @v_n_saldo_peca_media_pequena_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0),
               @v_n_saldo_peca_media_grande_total  = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0),
               @v_n_qtd_peca_media_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' OR c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN 1 END), 0),
               @v_n_qtd_nuance_total = ISNULL(COUNT(DISTINCT c.c_nuance), 0),
               @v_n_endereco_diferente = ISNULL(COUNT(DISTINCT c.c_endereco), 0),
               @v_n_ordem = MAX(n_ordem),
               @v_n_saldo_nuance_total = ISNULL(SUM(CASE WHEN c.c_nuance = @v_c_pai_nuance THEN c.n_saldo END), 0),
               @v_n_largura_min = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MAX(n_largura) - @v_n_regra_largura_variacao END,
               @v_n_largura_max = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MIN(n_largura) + @v_n_regra_largura_variacao END
          FROM @t_cenario c
         WHERE c.n_cenario = @v_n_cenario

        UPDATE t
           SET t.n_saldo_total_selecionado = c.n_saldo_total_selecionado,
               t.n_saldo_peca_media_pequena_selecionado = c.n_saldo_peca_media_pequena_selecionado,
               t.n_saldo_peca_media_grande_selecionado = c.n_saldo_peca_media_grande_selecionado
          FROM @t_nuance t
          JOIN (SELECT c.n_cenario,
                       c.c_nuance, 
                       ISNULL(SUM(c.n_saldo), 0) as n_saldo_total_selecionado,
                 ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0) as n_saldo_peca_media_pequena_selecionado,
                 ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0) as n_saldo_peca_media_grande_selecionado
                  FROM @t_cenario c
                   WHERE c.n_cenario = @v_n_cenario
             GROUP BY c.n_cenario, c.c_nuance) c
              ON c.n_cenario = t.n_cenario
           AND c.c_nuance = t.c_nuance
         WHERE t.n_cenario = @v_n_cenario

        SELECT @v_n_saldo_nuance_total = n.n_saldo_total_selecionado,
               @v_n_saldo_nuance_limite = n.n_saldo_total_limite,
               @v_n_saldo_peca_media_pequena_total = n.n_saldo_peca_media_pequena_selecionado,
               @v_n_saldo_peca_media_pequena_limite = n.n_saldo_peca_media_pequena_limite,
               @v_n_saldo_peca_media_grande_total = n.n_saldo_peca_media_grande_selecionado,
               @v_n_saldo_peca_media_grande_limite = n.n_saldo_peca_media_grande_limite
          FROM @t_nuance n
         WHERE n.n_cenario = @v_n_cenario
           AND n.c_nuance = @v_c_pai_nuance

        -- marca os lotes ja utilizados para desprezar nas proximas iteracoes
        UPDATE s 
           SET n_lote_ja_selecionado = 1
          FROM @t_saldo s
         WHERE exists (select 1 from @t_cenario c where c.n_cenario = @v_n_cenario and c.n_recno_d14 = s.n_recno_d14)

        -- Marcar registros com largura compativel
        UPDATE @t_saldo
           SET n_largura_ok = CASE 
                                WHEN @v_n_regra_largura_variacao IS NOT NULL THEN
                                  CASE 
                                    WHEN n_largura BETWEEN @v_n_largura_min AND @v_n_largura_max THEN
                                      1
                                    ELSE
                                      0
                                  END
                                ELSE 1
                              END
      END
    END

    --
    -- PROXIMA ITERACAO TENTA SELECIONAR O MAXIMO DE LOTES POSSIVEIS DE UMA SO VEZ
    -- Somente com a mesma nuance do pai
    -- Somente no mesmo endereco do pai
    -- Com variacao de tamanho para menor (@v_n_pai_largura - @v_n_regra_largura_variacao)
    --
    IF @v_n_saldo_total <  @v_n_qtd_desejada
   AND @v_n_saldo_nuance_total < @v_n_saldo_nuance_limite
    BEGIN

      SET @v_n_iteracao += 1

      -- Tenta buscar o maximo de linhas possiveis em mais uma iteracao
      INSERT INTO @t_cenario (n_cenario, n_iteracao, n_ordem, c_lote, c_endereco, c_nuance, n_largura, c_tipo_peca, n_saldo, n_recno_d14) 
      SELECT @v_n_cenario,
             @v_n_iteracao,
             n_ordem,
             c_lote, 
             c_endereco,
             c_nuance, 
             n_largura,
             c_tipo_peca,
             n_saldo, 
             n_recno_d14
        FROM (
  
      SELECT @v_n_ordem + ROW_NUMBER() OVER (ORDER BY n_largura_pai_diff, c_lote) as n_ordem,
             -- Soma cumulativa
             SUM(n_saldo) OVER (ORDER BY n_largura_pai_diff, c_lote ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_saldo_total,
             c_lote, 
             c_endereco,
             c_nuance, 
             n_largura,
             c_tipo_peca,
             n_saldo, 
             n_recno_d14           
        FROM (
  
      SELECT s.c_lote, 
             s.c_endereco,
             s.n_endereco_igual_pai,
             s.c_nuance, 
             s.n_largura,
             s.c_tipo_peca,
             s.n_saldo, 
             s.n_recno_d14,
             s.n_largura_pai_diff
        FROM @t_saldo s
           -- lote ainda nao selecionado
       WHERE s.n_lote_ja_selecionado = 0
         AND s.n_nuance_igual_pai = 1
         AND s.n_endereco_igual_pai = 1
         AND s.n_largura_ok = 1
         AND (@v_n_largura_min IS NULL OR s.n_largura >= @v_n_largura_min)
         AND (@v_n_largura_max IS NULL OR s.n_largura <= @v_n_pai_largura)
         AND s.c_tipo_peca = 'PECA NORMAL'             
             ) Q            
             ) Q2
       WHERE Q2.n_saldo_total <= (@v_n_regra_pedido_total_max - @v_n_saldo_total)
         AND (Q2.n_saldo_total + @v_n_saldo_nuance_total) <= @v_n_saldo_nuance_limite     
       ORDER BY n_ordem

      -- se conseguiu pescar um ou mais lotes
      IF @@ROWCOUNT > 0
      BEGIN
      -- Atualiza variaveis controladoras do saldo total selecionado
        SELECT @v_n_saldo_total = ISNULL(SUM(c.n_saldo), 0),
               @v_n_saldo_peca_normal_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA NORMAL' THEN c.n_saldo END), 0),
               @v_n_saldo_peca_media_pequena_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0),
               @v_n_saldo_peca_media_grande_total  = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0),
               @v_n_qtd_peca_media_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' OR c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN 1 END), 0),
               @v_n_qtd_nuance_total = ISNULL(COUNT(DISTINCT c.c_nuance), 0),
               @v_n_endereco_diferente = ISNULL(COUNT(DISTINCT c.c_endereco), 0),
               @v_n_ordem = MAX(n_ordem),
               @v_n_saldo_nuance_total = ISNULL(SUM(CASE WHEN c.c_nuance = @v_c_pai_nuance THEN c.n_saldo END), 0),
               @v_n_largura_min = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MAX(n_largura) - @v_n_regra_largura_variacao END,
               @v_n_largura_max = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MIN(n_largura) + @v_n_regra_largura_variacao END
          FROM @t_cenario c
         WHERE c.n_cenario = @v_n_cenario

        UPDATE t
           SET t.n_saldo_total_selecionado = c.n_saldo_total_selecionado,
               t.n_saldo_peca_media_pequena_selecionado = c.n_saldo_peca_media_pequena_selecionado,
               t.n_saldo_peca_media_grande_selecionado = c.n_saldo_peca_media_grande_selecionado
          FROM @t_nuance t
          JOIN (SELECT c.n_cenario,
                       c.c_nuance, 
                       ISNULL(SUM(c.n_saldo), 0) as n_saldo_total_selecionado,
                 ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0) as n_saldo_peca_media_pequena_selecionado,
                 ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0) as n_saldo_peca_media_grande_selecionado
                  FROM @t_cenario c
                   WHERE c.n_cenario = @v_n_cenario
             GROUP BY c.n_cenario, c.c_nuance) c
              ON c.n_cenario = t.n_cenario
           AND c.c_nuance = t.c_nuance
         WHERE t.n_cenario = @v_n_cenario

        SELECT @v_n_saldo_nuance_total = n.n_saldo_total_selecionado,
               @v_n_saldo_nuance_limite = n.n_saldo_total_limite,
               @v_n_saldo_peca_media_pequena_total = n.n_saldo_peca_media_pequena_selecionado,
               @v_n_saldo_peca_media_pequena_limite = n.n_saldo_peca_media_pequena_limite,
               @v_n_saldo_peca_media_grande_total = n.n_saldo_peca_media_grande_selecionado,
               @v_n_saldo_peca_media_grande_limite = n.n_saldo_peca_media_grande_limite
          FROM @t_nuance n
         WHERE n.n_cenario = @v_n_cenario
           AND n.c_nuance = @v_c_pai_nuance

        -- marca os lotes ja utilizados para desprezar nas proximas iteracoes
        UPDATE s 
           SET n_lote_ja_selecionado = 1
          FROM @t_saldo s
         WHERE exists (select 1 from @t_cenario c where c.n_cenario = @v_n_cenario and c.n_recno_d14 = s.n_recno_d14)

        -- Marcar registros com largura compativel
        UPDATE @t_saldo
           SET n_largura_ok = CASE 
                                WHEN @v_n_regra_largura_variacao IS NOT NULL THEN
                                  CASE 
                                    WHEN n_largura BETWEEN @v_n_largura_min AND @v_n_largura_max THEN
                                      1
                                    ELSE
                                      0
                                  END
                                ELSE 1
                              END         
      END
    END  

    --
    -- PROXIMA ITERACAO TENTA SELECIONAR O MAXIMO DE LOTES POSSIVEIS DE UMA SO VEZ
    -- Somente com a mesma nuance do pai
    -- Somente em enderecos diferentes do pai
    --
    IF @v_n_saldo_total <  @v_n_qtd_desejada
   AND @v_n_saldo_nuance_total < @v_n_saldo_nuance_limite
    BEGIN

      SET @v_n_iteracao += 1

      -- Tenta buscar o maximo de linhas possiveis em mais uma iteracao
      INSERT INTO @t_cenario (n_cenario, n_iteracao, n_ordem, c_lote, c_endereco, c_nuance, n_largura, c_tipo_peca, n_saldo, n_recno_d14) 
      SELECT @v_n_cenario,
             @v_n_iteracao,
             n_ordem,
             c_lote, 
             c_endereco,
             c_nuance, 
             n_largura,
             c_tipo_peca,
             n_saldo, 
             n_recno_d14
        FROM (
  
      SELECT @v_n_ordem + ROW_NUMBER() OVER (ORDER BY c_endereco, n_largura_pai_diff, c_lote) as n_ordem,
             -- Soma cumulativa
             SUM(n_saldo) OVER (ORDER BY c_endereco, n_largura_pai_diff, c_lote ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_saldo_total,
             c_lote, 
             c_endereco,
             c_nuance, 
             n_largura,
             c_tipo_peca,
             n_saldo, 
             n_recno_d14           
        FROM (
  
      SELECT s.c_lote, 
             s.c_endereco,
             s.n_endereco_igual_pai,
             s.c_nuance, 
             s.n_largura,
             s.c_tipo_peca,
             s.n_saldo, 
             s.n_recno_d14,
             s.n_largura_pai_diff
        FROM @t_saldo s
           -- lote ainda nao selecionado
       WHERE s.n_lote_ja_selecionado = 0
         AND s.n_nuance_igual_pai = 1
         AND s.n_endereco_igual_pai = 0
         AND s.n_largura_ok = 1
         AND s.c_tipo_peca = 'PECA NORMAL'             
             ) Q            
             ) Q2
       WHERE Q2.n_saldo_total <= (@v_n_regra_pedido_total_max - @v_n_saldo_total)
         AND (Q2.n_saldo_total + @v_n_saldo_nuance_total) <= @v_n_saldo_nuance_limite     
       ORDER BY n_ordem

      -- se conseguiu pescar um ou mais lotes
      IF @@ROWCOUNT > 0
      BEGIN
      -- Atualiza variaveis controladoras do saldo total selecionado
        SELECT @v_n_saldo_total = ISNULL(SUM(c.n_saldo), 0),
               @v_n_saldo_peca_normal_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA NORMAL' THEN c.n_saldo END), 0),
               @v_n_saldo_peca_media_pequena_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0),
               @v_n_saldo_peca_media_grande_total  = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0),
               @v_n_qtd_peca_media_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' OR c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN 1 END), 0),
               @v_n_qtd_nuance_total = ISNULL(COUNT(DISTINCT c.c_nuance), 0),
               @v_n_endereco_diferente = ISNULL(COUNT(DISTINCT c.c_endereco), 0),
               @v_n_ordem = MAX(n_ordem),
               @v_n_saldo_nuance_total = ISNULL(SUM(CASE WHEN c.c_nuance = @v_c_pai_nuance THEN c.n_saldo END), 0),
               @v_n_largura_min = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MAX(n_largura) - @v_n_regra_largura_variacao END,
               @v_n_largura_max = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MIN(n_largura) + @v_n_regra_largura_variacao END
          FROM @t_cenario c
         WHERE c.n_cenario = @v_n_cenario

        UPDATE t
           SET t.n_saldo_total_selecionado = c.n_saldo_total_selecionado,
               t.n_saldo_peca_media_pequena_selecionado = c.n_saldo_peca_media_pequena_selecionado,
               t.n_saldo_peca_media_grande_selecionado = c.n_saldo_peca_media_grande_selecionado
          FROM @t_nuance t
          JOIN (SELECT c.n_cenario,
                       c.c_nuance, 
                       ISNULL(SUM(c.n_saldo), 0) as n_saldo_total_selecionado,
                       ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0) as n_saldo_peca_media_pequena_selecionado,
                       ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0) as n_saldo_peca_media_grande_selecionado
                  FROM @t_cenario c
                 WHERE c.n_cenario = @v_n_cenario
                 GROUP BY c.n_cenario, c.c_nuance) c
              ON c.n_cenario = t.n_cenario
           AND c.c_nuance = t.c_nuance
         WHERE t.n_cenario = @v_n_cenario

        SELECT @v_n_saldo_nuance_total = n.n_saldo_total_selecionado,
               @v_n_saldo_nuance_limite = n.n_saldo_total_limite,
               @v_n_saldo_peca_media_pequena_total = n.n_saldo_peca_media_pequena_selecionado,
               @v_n_saldo_peca_media_pequena_limite = n.n_saldo_peca_media_pequena_limite,
               @v_n_saldo_peca_media_grande_total = n.n_saldo_peca_media_grande_selecionado,
               @v_n_saldo_peca_media_grande_limite = n.n_saldo_peca_media_grande_limite
          FROM @t_nuance n
         WHERE n.n_cenario = @v_n_cenario
           AND n.c_nuance = @v_c_pai_nuance

        -- marca os lotes ja utilizados para desprezar nas proximas iteracoes
        UPDATE s 
           SET n_lote_ja_selecionado = 1
          FROM @t_saldo s
         WHERE exists (select 1 from @t_cenario c where c.n_cenario = @v_n_cenario and c.n_recno_d14 = s.n_recno_d14)

        -- Marcar registros com largura compativel
        UPDATE @t_saldo
           SET n_largura_ok = CASE 
                                WHEN @v_n_regra_largura_variacao IS NOT NULL THEN
                                  CASE 
                                    WHEN n_largura BETWEEN @v_n_largura_min AND @v_n_largura_max THEN
                                      1
                                    ELSE
                                      0
                                  END
                                ELSE 1
                              END         
      END
    END    
   
    --
    -- PROXIMA ITERACAO E DEMAIS TENTA PESCAR LOTE A LOTE
    --
    WHILE (@v_n_saldo_total < @v_n_qtd_desejada)
    BEGIN
      SET @v_n_ordem += 1
      SET @v_n_iteracao += 1

      SELECT TOP 1 @v_c_filho_lote = s.c_lote, 
                   @v_c_filho_endereco = s.c_endereco,
                   @v_c_filho_nuance = s.c_nuance, 
                   @v_n_filho_largura = s.n_largura,
                   @v_n_filho_saldo = s.n_saldo, 
                   @v_c_filho_tipo_peca = s.c_tipo_peca, 
                   @v_n_filho_recno = s.n_recno_d14
        FROM @t_saldo s
         -- lote ainda nao selecionado
       WHERE s.n_lote_ja_selecionado = 0
         -- avalia se e possivel selecionar nuance diferente
         AND (@v_n_qtd_nuance_total < ISNULL(@v_n_regra_nuance_max, @v_n_qtd_nuance_total + 1) OR EXISTS (select 1 from @t_nuance n where n.n_cenario = @v_n_cenario AND n.c_nuance = s.c_nuance))        
         -- nao ultrapassar do total desejado no pedido
         AND s.n_saldo <= (@v_n_regra_pedido_total_max - @v_n_saldo_total)
         AND s.n_largura_ok = 1
       ORDER BY -- Prioriza mesma nuance
                s.n_nuance_igual_pai DESC,
                s.c_nuance,
                -- Tenta preecher q quantidade exata
                ABS(s.n_saldo - (@v_n_qtd_desejada - @v_n_saldo_total)),
                -- ENDERECO MAIS PROXIMO
                n_endereco_igual_pai DESC,
                -- Procura pelas menores diferencas de largura
                ABS(s.n_largura - @v_n_pai_largura),
                c_endereco, 
                -- FIFO
                s.c_lote
    
      -- Se nao houver lote que atenda aos criterios, sai do loop
      IF (@@ROWCOUNT = 0)
         BREAK

      SELECT @v_n_saldo_nuance_total = n.n_saldo_total_selecionado,
             @v_n_saldo_nuance_limite = n.n_saldo_total_limite,
             @v_n_saldo_peca_media_pequena_total = n.n_saldo_peca_media_pequena_selecionado,       
             @v_n_saldo_peca_media_pequena_limite = n.n_saldo_peca_media_pequena_limite,
             @v_n_saldo_peca_media_grande_total = n.n_saldo_peca_media_grande_selecionado,
             @v_n_saldo_peca_media_grande_limite = n.n_saldo_peca_media_grande_limite
        FROM @t_nuance n
       WHERE n.n_cenario = @v_n_cenario
         AND n.c_nuance = @v_c_filho_nuance
    
      -- Se nao encontrar a nuance cria nova linhaa
      IF (@@ROWCOUNT = 0)
         INSERT INTO @t_nuance 
             (n_cenario,
              c_nuance, 
              n_saldo_total_disponivel, 
              n_saldo_total_selecionado, 
              n_saldo_total_limite, 
              n_saldo_peca_media_pequena_selecionado,
              n_saldo_peca_media_pequena_limite,
              n_saldo_peca_media_grande_selecionado,
              n_saldo_peca_media_grande_limite) 
       SELECT @v_n_cenario,
              s.c_nuance, 
              SUM(s.n_saldo) AS n_saldo_total_disponivel,
              @v_n_filho_saldo as n_saldo_total_selecionado,
              CASE 
                WHEN SUM(s.n_saldo) > @v_n_qtd_desejada THEN 
                SUM(s.n_saldo)
              ELSE
                (@v_n_qtd_desejada - @v_n_saldo_minimo_nuance)
              END as n_saldo_total_limite,
              SUM(CASE WHEN @v_c_filho_tipo_peca = 'PECA MEDIA PEQUENA' THEN s.n_saldo ELSE 0 END) as n_saldo_peca_media_pequena_selecionado,
              CASE
                WHEN @v_n_regra_nuance_max > 0 THEN
                  @v_n_qtd_desejada * (@p_n_peca_media_pequena_limite_saldo / @v_n_regra_nuance_max) 
              END as n_saldo_peca_media_pequena_limite,
              SUM(CASE WHEN @v_c_filho_tipo_peca = 'PECA MEDIA GRANDE' THEN s.n_saldo ELSE 0 END) as n_saldo_peca_media_grande_selecionado,
              CASE
                WHEN @v_n_regra_nuance_max > 0 THEN
                  @v_n_qtd_desejada * (@p_n_peca_media_grande_limite_saldo / @v_n_regra_nuance_max) 
              END as n_saldo_peca_media_grande_limite
         FROM @t_saldo s
        WHERE s.c_nuance = @v_c_filho_nuance
          AND s.n_largura_ok = 1
        GROUP BY s.c_nuance
       
      IF (@v_n_filho_saldo + @v_n_saldo_nuance_total) <= @v_n_saldo_nuance_limite
     AND (@v_n_saldo_peca_media_pequena_limite IS NULL OR ((CASE WHEN @v_c_filho_tipo_peca = 'PECA MEDIA PEQUENA' THEN @v_n_filho_saldo ELSE 0 END) + @v_n_saldo_peca_media_pequena_total) <= @v_n_saldo_peca_media_pequena_limite)
     AND (@v_n_saldo_peca_media_grande_limite IS NULL OR ((CASE WHEN @v_c_filho_tipo_peca = 'PECA MEDIA GRANDE' THEN @v_n_filho_saldo ELSE 0 END) +  @v_n_saldo_peca_media_grande_total) <= @v_n_saldo_peca_media_grande_limite)
      BEGIN
        INSERT INTO @t_cenario (n_cenario, n_iteracao, n_ordem, c_lote, c_endereco, c_nuance, n_largura, n_saldo, c_tipo_peca, n_recno_d14) 
        VALUES (@v_n_cenario, @v_n_iteracao, @v_n_ordem, @v_c_filho_lote, @v_c_filho_endereco, @v_c_filho_nuance, @v_n_filho_largura, @v_n_filho_saldo, @v_c_filho_tipo_peca, @v_n_filho_recno)

        -- Atualiza variaveis controladoras do saldo total selecionado
        SELECT @v_n_saldo_total = ISNULL(SUM(c.n_saldo), 0),
               @v_n_saldo_peca_normal_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA NORMAL' THEN c.n_saldo END), 0),
               @v_n_saldo_peca_media_pequena_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0),
               @v_n_saldo_peca_media_grande_total  = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0),
               @v_n_qtd_peca_media_total = ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' OR c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN 1 END), 0),
               @v_n_qtd_nuance_total = ISNULL(COUNT(DISTINCT c.c_nuance), 0),
               @v_n_endereco_diferente = ISNULL(COUNT(DISTINCT c.c_endereco), 0),
               @v_n_ordem = MAX(n_ordem),
               @v_n_largura_min = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MAX(n_largura) - @v_n_regra_largura_variacao END,
               @v_n_largura_max = CASE WHEN @v_n_regra_largura_variacao IS NOT NULL THEN MIN(n_largura) + @v_n_regra_largura_variacao END
          FROM @t_cenario c
         WHERE c.n_cenario = @v_n_cenario

        UPDATE t
           SET t.n_saldo_total_selecionado = c.n_saldo_total_selecionado,
               t.n_saldo_peca_media_pequena_selecionado = c.n_saldo_peca_media_pequena_selecionado,
               t.n_saldo_peca_media_grande_selecionado = c.n_saldo_peca_media_grande_selecionado
          FROM @t_nuance t
          JOIN (SELECT c.n_cenario,
                       c.c_nuance, 
                       ISNULL(SUM(c.n_saldo), 0) as n_saldo_total_selecionado,
                       ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA PEQUENA' THEN c.n_saldo END), 0) as n_saldo_peca_media_pequena_selecionado,
                       ISNULL(SUM(CASE WHEN c.c_tipo_peca = 'PECA MEDIA GRANDE' THEN c.n_saldo END), 0) as n_saldo_peca_media_grande_selecionado
                  FROM @t_cenario c
                 WHERE c.n_cenario = @v_n_cenario
                 GROUP BY c.n_cenario, c.c_nuance) c
            ON c.n_cenario = t.n_cenario
           AND c.c_nuance = t.c_nuance
         WHERE t.n_cenario = @v_n_cenario

        SELECT @v_n_saldo_nuance_total = n.n_saldo_total_selecionado,
               @v_n_saldo_nuance_limite = n.n_saldo_total_limite,
               @v_n_saldo_peca_media_pequena_total = n.n_saldo_peca_media_pequena_selecionado,
               @v_n_saldo_peca_media_pequena_limite = n.n_saldo_peca_media_pequena_limite,
               @v_n_saldo_peca_media_grande_total = n.n_saldo_peca_media_grande_selecionado,
               @v_n_saldo_peca_media_grande_limite = n.n_saldo_peca_media_grande_limite
          FROM @t_nuance n
         WHERE n.n_cenario = @v_n_cenario
           AND n.c_nuance = @v_c_pai_nuance        
      END

        UPDATE s 
           SET n_lote_ja_selecionado = 1
          FROM @t_saldo s
         WHERE s.n_recno_d14 = @v_n_filho_recno

        -- Marcar registros com largura compativel
        UPDATE @t_saldo
           SET n_largura_ok = CASE 
                                WHEN @v_n_regra_largura_variacao IS NOT NULL THEN
                                  CASE 
                                    WHEN n_largura BETWEEN @v_n_largura_min AND @v_n_largura_max THEN
                                      1
                                    ELSE
                                      0
                                  END
                                ELSE 1
                              END         
      END 

      UPDATE @t_cenario
         SET n_saldo_total = @v_n_saldo_total,
             n_qtd_peca_media_total = @v_n_qtd_peca_media_total,
             n_qtd_nuance_total = @v_n_qtd_nuance_total,
             n_endereco_diferente = @v_n_endereco_diferente
       WHERE n_cenario = @v_n_cenario

      -- Variacao de largura
      IF @v_n_regra_largura_variacao IS NOT NULL
      BEGIN
        UPDATE c
           SET n_largura_min = (SELECT MIN(c1.n_largura) FROM @t_cenario c1 WHERE c1.n_cenario = c.n_cenario),
               n_largura_max = (SELECT MAX(c1.n_largura) FROM @t_cenario c1 WHERE c1.n_cenario = c.n_cenario)
          FROM @t_cenario c
         WHERE n_cenario = @v_n_cenario
      
        UPDATE c
           SET c.n_largura_variacao = c.n_largura_max - c.n_largura_min
          FROM @t_cenario c
         WHERE n_cenario = @v_n_cenario
      END
    
    UPDATE c
       SET c.n_saldo_nuance_total = (SELECT n.n_saldo_total_selecionado FROM @t_nuance n WHERE n.n_cenario = c.n_cenario AND n.c_nuance = c.c_nuance)
      FROM @t_cenario c
     WHERE c.n_cenario = @v_n_cenario
             
    SELECT TOP 1 @v_n_cenario_selecionado = c.n_cenario
      FROM @t_cenario c
     WHERE c.n_cenario = @v_n_cenario
     --AND c.n_saldo_total = @v_n_qtd_desejada
     AND c.n_saldo_total BETWEEN @v_n_regra_pedido_total_min AND @v_n_regra_pedido_total_max
     AND (@v_n_regra_largura_variacao IS NULL OR c.n_largura_variacao <= @v_n_regra_largura_variacao)
       AND NOT EXISTS (SELECT 1 
                         FROM @t_cenario d 
                        WHERE d.n_cenario = c.n_cenario
                          AND d.n_saldo_nuance_total < @v_n_saldo_minimo_nuance)
     AND (@p_n_troca_lote = 0 OR EXISTS (SELECT 1 FROM @t_cenario c1 WHERE c1.n_cenario = c.n_cenario AND c1.n_recno_d14 = @v_n_recno_d14_lote_novo))

    -- Se houver cenario que atenda aos criterios, sai do loop
    IF @v_n_cenario_selecionado IS NOT NULL
       BREAK

    SET @v_n_cenario += 1

    FETCH NEXT 
     FROM C_LOTE
     INTO @v_c_pai_lote, @v_n_pai_recno, @v_c_pai_nuance, @v_c_pai_endereco, @v_c_pai_tipo_peca, @v_n_pai_largura, @v_n_pai_saldo
  
  END
  CLOSE C_LOTE
  DEALLOCATE C_LOTE
  
  IF @v_n_cenario_selecionado IS NULL
  BEGIN
    -- sintetiza os cenarios para calcular os fatores, somar a pontuacao e ordenar para selecionar o melhor cenario
    INSERT INTO @t_cenario_sintetico
          (n_cenario,
           n_qtd_peca_media_total,
           n_qtd_nuance_total,
           n_endereco_diferente,
           n_qtd_desejada_diff)
    SELECT n_cenario,
           n_qtd_peca_media_total,
           n_qtd_nuance_total,
           n_endereco_diferente,
           ABS(@v_n_qtd_desejada - c.n_saldo_total) AS n_qtd_desejada_diff
      FROM @t_cenario c
     WHERE (c.n_saldo_total BETWEEN @v_n_regra_pedido_total_min AND @v_n_regra_pedido_total_max)
       AND (@v_n_regra_largura_variacao IS NULL OR c.n_largura_variacao <= @v_n_regra_largura_variacao)
       AND NOT EXISTS (SELECT 1 
                         FROM @t_cenario d 
                        WHERE d.n_cenario = c.n_cenario
                          AND d.n_saldo_nuance_total < @v_n_saldo_minimo_nuance)  
       AND (@p_n_troca_lote = 0 OR EXISTS (SELECT 1 FROM @t_cenario c1 WHERE c1.n_cenario = c.n_cenario AND c1.n_recno_d14 = @v_n_recno_d14_lote_novo))
     GROUP BY n_cenario,
              n_qtd_peca_media_total,
              n_qtd_nuance_total,
              n_endereco_diferente,
              c.n_saldo_total

    SELECT @v_n_endereco_diferente_min = MIN(s.n_endereco_diferente),
           @v_n_endereco_diferente_max = MAX(s.n_endereco_diferente),
           @v_n_qtd_nuance_total_min = MIN(s.n_qtd_nuance_total),
           @v_n_qtd_nuance_total_max = MAX(s.n_qtd_nuance_total),
           @v_n_qtd_peca_media_total_min = MIN(s.n_qtd_peca_media_total),
           @v_n_qtd_peca_media_total_max = MAX(s.n_qtd_peca_media_total),
           @v_n_qtd_desejada_diff_min = MIN(s.n_qtd_desejada_diff),
           @v_n_qtd_desejada_diff_max = MAX(s.n_qtd_desejada_diff) 
      FROM @t_cenario_sintetico s

    UPDATE s
         -- Quanto menos nuances melhor
     SET s.n_qtd_nuance_total_fator = 1 - ((s.n_qtd_nuance_total - @v_n_qtd_nuance_total_min) / CASE WHEN @v_n_qtd_nuance_total_max - @v_n_qtd_nuance_total_min > 0 THEN @v_n_qtd_nuance_total_max - @v_n_qtd_nuance_total_min ELSE 1 END),
         -- Quanto mais peca media melhor
         s.n_qtd_peca_media_total_fator = ((s.n_qtd_peca_media_total - @v_n_qtd_peca_media_total_min) / CASE WHEN @v_n_qtd_peca_media_total_max - @v_n_qtd_peca_media_total_min > 0 THEN @v_n_qtd_peca_media_total_max - @v_n_qtd_peca_media_total_min ELSE 1 END),
         -- Quanto menos enderecos melhor,
         s.n_endereco_diferente_fator = 1 - ((s.n_endereco_diferente - @v_n_endereco_diferente_min) / CASE WHEN @v_n_endereco_diferente_max - @v_n_endereco_diferente_min > 0 THEN @v_n_endereco_diferente_max - @v_n_endereco_diferente_min ELSE 1 END),
         -- Quanto menor a diferenca entre o saldo total do cenario e a quantidade desejada no pedido melhor
         s.n_qtd_desejada_diff_fator = 1 - ((s.n_qtd_desejada_diff - @v_n_qtd_desejada_diff_min) / CASE WHEN @v_n_qtd_desejada_diff_max - @v_n_qtd_desejada_diff_min > 0 THEN @v_n_qtd_desejada_diff_max - @v_n_qtd_desejada_diff_min ELSE 1 END)
    FROM @t_cenario_sintetico s

    UPDATE @t_cenario_sintetico
       SET n_ordem_tentativa = (n_qtd_nuance_total_fator * 3) 
                           + n_qtd_peca_media_total_fator + 
               + (n_endereco_diferente_fator * 2) 
               + n_qtd_desejada_diff_fator

    SELECT TOP 1 @v_n_cenario_selecionado = c.n_cenario
      FROM @t_cenario_sintetico c
     ORDER BY c.n_ordem_tentativa DESC
  END

  SELECT *
    FROM @t_regra r
   ORDER BY r.v_c_regra_tipo, r.v_n_regra_de

  -- Regra para limitar quantidade de nuances diferentes que pode ser enviada
  SELECT @v_n_regra_nuance_max AS v_n_regra_nuance_max,
         @v_n_regra_peca_media_qtd_max AS v_n_regra_peca_media_qtd_max,
         @v_n_regra_peca_media_pequena_saldo_max AS v_n_regra_peca_media_pequena_saldo_max,
         @v_n_regra_peca_media_grande_saldo_max AS v_n_regra_peca_media_grande_saldo_max,
         @v_n_regra_peca_media_saldo_max AS v_n_regra_peca_media_saldo_max,
         @v_n_regra_pedido_variacao_de AS v_n_regra_pedido_variacao_de,
         @v_n_regra_pedido_variacao_ate AS v_n_regra_pedido_variacao_ate,
         @v_n_regra_pedido_total_min AS v_n_regra_pedido_total_min,
         @v_n_regra_pedido_total_max AS v_n_regra_pedido_total_max,
         @v_n_regra_rolo_metro_de AS v_n_regra_rolo_metro_de,
         @v_n_regra_rolo_metro_ate AS v_n_regra_rolo_metro_ate,
         @v_n_regra_largura_de AS v_n_regra_rolo_largura_de,
         @v_n_regra_largura_ate AS v_n_regra_rolo_largura_ate,
         @v_n_regra_largura_variacao AS v_n_regra_variacao_largura,
         @v_n_regra_rolo_peso_de AS v_n_regra_rolo_peso_de,
         @v_n_regra_rolo_peso_ate AS v_n_regra_rolo_peso_ate

  SELECT * 
    FROM @t_saldo
   ORDER BY c_nuance, c_lote

  SELECT * 
    from @t_saldo_nuance
   ORDER BY n_ordem_tentativa DESC, c_nuance

  SELECT *
    FROM @t_nuance
   ORDER BY n_cenario, c_nuance

  SELECT *, @v_n_qtd_desejada - c.n_saldo_total as n_diff
    FROM @t_cenario c
   ORDER BY n_cenario, n_ordem

  SELECT *
    FROM @t_cenario_sintetico
   ORDER BY n_ordem_tentativa DESC

  SELECT *
    FROM @t_cenario
   WHERE n_cenario = @v_n_cenario_selecionado
   ORDER BY n_cenario, n_ordem

  --Se encontrou cenario monta a tabela com o resultado para o Protheus
  IF @v_n_cenario_selecionado IS NOT NULL
     SELECT DC3_ORDEM,
            D14_ENDER,
            D14_ESTFIS,
            D14_LOTECT,
            D14_NUMLOT,
            D14_DTVALD,
            D14_NUMSER,
            (D14_QTDEST-(D14_QTDEMP+D14_QTDBLQ)) D14_QTDLIB,
            (D14_QTDEST-(D14_QTDEMP+D14_QTDBLQ+D14_QTDSPR)) D14_SALDO,
            D14_QTDSPR,
            D14_IDUNIT,
            D14_CODUNI
       FROM @t_cenario c
     JOIN D14010 AS D14 WITH (NOLOCK)
       ON D14.R_E_C_N_O_ = c.n_recno_d14
    AND D14.D_E_L_E_T_ <> '*'
       JOIN DC3010 AS DC3 WITH (NOLOCK)
         ON DC3.DC3_FILIAL = @v_c_filial_dc3
        AND DC3.DC3_LOCAL  = D14.D14_LOCAL
        AND DC3.DC3_CODPRO = D14.D14_PRODUT
        AND DC3.DC3_TPESTR = D14.D14_ESTFIS
        AND DC3.D_E_L_E_T_ <> '*'
       JOIN DC8010 AS DC8 WITH (NOLOCK)
         ON DC8.DC8_FILIAL = @v_c_filial_dc8
        AND DC8.DC8_CODEST = D14.D14_ESTFIS
        AND DC8.DC8_TPESTR IN ('1','2','3','4','6')
        AND DC8.D_E_L_E_T_ <> '*'
    WHERE c.n_cenario = @v_n_cenario_selecionado        
    ORDER BY c.c_nuance, c.c_endereco
    ELSE
      SELECT ' ' DC3_ORDEM,
             ' ' D14_ENDER,
             ' ' D14_ESTFIS,
             ' ' D14_LOTECT,
             ' ' D14_NUMLOT,
             ' ' D14_DTVALD,
             ' ' D14_NUMSER,
             0   D14_QTDLIB,
             0   D14_SALDO,
             0   D14_QTDSPR,
             ' ' D14_IDUNIT,
             ' ' D14_CODUNI
END


GO


