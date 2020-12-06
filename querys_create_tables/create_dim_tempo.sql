DROP TABLE IF EXISTS dw.dim_tempo;
CREATE TABLE IF NOT EXISTS dw.dim_tempo
(
  date_dim_id              		INT NOT NULL,
  date_atual               		DATE NOT NULL,
  epoca                    		BIGINT NOT NULL,
  dia_sufixo               		VARCHAR(4) NOT NULL,
  dia_nome                 		VARCHAR(9) NOT NULL,
  dia_da_semana            		INT NOT NULL,
  dia_do_mes               		INT NOT NULL,
  dia_do_trimestre         		INT NOT NULL,
  dia_do_ano               		INT NOT NULL,
  semana_do_mes            		INT NOT NULL,
  semana_do_ano             	INT NOT NULL,
  semana_do_ano_iso         	CHAR(10) NOT NULL,
  mes_atual             		  INT NOT NULL,
  mes_nome               		  VARCHAR(9) NOT NULL,
  mes_nome_abreviado   			  CHAR(3) NOT NULL,
  trimestre_atual           	INT NOT NULL,
  trimestre_nome             	VARCHAR(9) NOT NULL,
  ano_atual              	  	INT NOT NULL,
  primeiro_dia_da_semana      DATE NOT NULL,
  ultimo_dia_da_semana        DATE NOT NULL,
  primeiro_dia_do_mes       	DATE NOT NULL,
  ultimo_dia_do_mes        		DATE NOT NULL,
  primeiro_dia_do_trimestre   DATE NOT NULL,
  ultimo_dia_do_trimestre     DATE NOT NULL,
  primeiro_dia_do_ano        	DATE NOT NULL,
  ultimo_dia_do_ano         	DATE NOT NULL,
  mmyyyy                   		CHAR(7) NOT NULL,
  ddmmyyyy                 		CHAR(12) NOT NULL,
  mmddyyyy                 		CHAR(12) NOT NULL,
  fim_de_semana             	BOOLEAN NOT NULL
);


ALTER TABLE dw.dim_tempo ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX d_date_date_actual_idx
  ON dw.dim_tempo(date_atual);

COMMIT;


INSERT INTO dw.dim_tempo
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
       datum AS date_atual,
       EXTRACT(EPOCH FROM datum) AS epoca,
       TO_CHAR(datum, 'fmDDth') AS dia_sufixo,
       TO_CHAR(datum, 'Day') AS dia_nome,
       EXTRACT(ISODOW FROM datum) AS dia_da_semana,
       EXTRACT(DAY FROM datum) AS dia_do_mes,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS dia_do_trimestre,
       EXTRACT(DOY FROM datum) AS dia_do_ano,
       TO_CHAR(datum, 'W')::INT AS semana_do_mes,
       EXTRACT(WEEK FROM datum) AS semana_do_ano,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS semana_do_ano_iso,
       EXTRACT(MONTH FROM datum) AS mes_atual,
       TO_CHAR(datum, 'Month') AS mes_nome,
       TO_CHAR(datum, 'Mon') AS mes_nome_abreviado,
       EXTRACT(QUARTER FROM datum) AS trimestre_atual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS trimestre_nome,
       EXTRACT(ISOYEAR FROM datum) AS ano_atual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS primeiro_dia_da_semana,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS ultimo_dia_da_semana,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS primeiro_dia_do_mes,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS ultimo_dia_do_mes,
       DATE_TRUNC('quarter', datum)::DATE AS primeiro_dia_do_trimestre,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS ultimo_dia_do_trimestre,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS primeiro_dia_do_ano,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS ultimo_dia_do_ano,
       TO_CHAR(datum, 'mm-yyyy') AS mmyyyy,
       TO_CHAR(datum, 'dd-mm-yyyy') AS ddmmyyyy,
	   TO_CHAR(datum, 'mm-dd-yyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS fim_de_semana
FROM (SELECT '1990-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;
COMMIT;