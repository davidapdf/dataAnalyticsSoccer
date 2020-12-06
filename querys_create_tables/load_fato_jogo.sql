create table if not exists dw.f_jogos (
	sk_data integer not null, 
	sk_arena integer not null, 
	sk_estado integer not null,
	sk_time integer not null, 
	rodada varchar(30), 
	lugar varchar(30), 
	statuspartida varchar(30),
	qtd_gols integer,
	qtd_gols_sofridos integer,
	media_gols integer,
	media_gols_sofrido integer,
	dt_insert  timestamp DEFAULT   CAST(NOW() AS TIMESTAMP),
	user_insert  varchar(30) DEFAULT  current_user ) ;


drop table if exists  tb_full2;
select 
--row_number() OVER (ORDER BY data,rodada,clube,arena) as Row, 
case when rodada = '1ª RODADA' then 
	to_char(data, 'YYYY')
else
	lag(to_char(data, 'YYYY'),4) over (order by data,rodada,clube,arena) end as data_ant , * 
into tb_full2
from (
select 
count(*) as qtd_partidas,
clube1 as clube,
arena,
clube1estado as estado,
data,
rodada,
'Casa' as Lugar,
case 
	 when vencedor = '-' then 'Empate'
	 when clube1 = vencedor then 'Vitoria'
	 when clube2 = vencedor then 'Derrota'
end as StatusPartida,
sum(clube1gols) as qtd_gols,
sum(clube2gols) as qtd_gols_sofridos,
round(avg(clube1gols),2) as media_gols,
round(avg(clube2gols),2) as media_gols_sofridos
from stg.campeonatobrasileirofull as tb_full
group by 
case 
	 when vencedor = '-' then 'Empate'
	 when clube1 = vencedor then 'Vitoria'
	 when clube2 = vencedor then 'Derrota'
end,
clube1,
arena,
clube1estado,
rodada,
data
UNION ALL
select 
count(*) as qtd_partidas,
clube2 as clube,
arena,
clube2estado as estado,
data,
rodada,
'Fora de casa' as Lugar,
case 
	 when vencedor = '-' then 'Empate'
	 when clube2 = vencedor then 'Vitoria'
	 when clube1 = vencedor then 'Derrota'
end as StatusPartida,
sum(clube2gols) as qtd_gols,
sum(clube1gols) as qtd_gols_sofridos,
round(avg(clube2gols),2) as media_gols,
round(avg(clube1gols),2) as media_gols_sofridos
from stg.campeonatobrasileirofull as tb_full
group by 
case 
	 when vencedor = '-' then 'Empate'
	 when clube2 = vencedor then 'Vitoria'
	 when clube1 = vencedor then 'Derrota'
end,
clube2,
arena,
clube2estado,
rodada,
data) as tb_full
order by data,rodada,clube;


delete from dw.f_jogos;

insert into dw.f_jogos  (
sk_data,
sk_arena,
sk_estado,
sk_time,
rodada,
lugar,
statuspartida,
qtd_gols,
qtd_gols_sofridos,
media_gols,
media_gols_sofrido)

select 
tempo.date_dim_id as sk_data, --Normal Dimension
arena.sk_id as sk_arena, --Normal Dimension
estado.sk_id as sk_estado, --Normal Dimension
time.sk_id as sk_time, --Normal Dimension
tb_full.rodada as rodada,--Stacked dimension
tb_full.lugar as lugar, --Stacked dimension
tb_full.statuspartida as statuspartida, --result of fact 
tb_full.qtd_gols as qtd_gols, --result of fact 
tb_full.qtd_gols_sofridos as qtd_gols_sofridos, --result of fact 
tb_full.media_gols as media_gols,-- result of fact 
tb_full.media_gols_sofridos as medias_gols_sofridos --result of fact 
from tb_full2 as tb_full
inner join dw.dim_tempo as tempo on (tb_full.data = tempo.date_atual) 
inner join dw.dim_arena as arena on (tb_full.clube = arena.clube and tb_full.arena = arena.arena)
inner join dw.dim_estado as estado on (estado.clube = tb_full.clube and estado.estado = tb_full.estado)
inner join dw.dim_time as time on (time.clube = tb_full.clube);
