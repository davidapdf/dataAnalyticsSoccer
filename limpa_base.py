from dao import set

query = """delete from dw.dim_arena; 
    ALTER SEQUENCE dw.sq_dim_arena_sk_id RESTART;
delete from dw.dim_estado; 
    ALTER SEQUENCE dw.sq_dim_estado_sk_id RESTART;
delete from dw.dim_time; 
    ALTER SEQUENCE dw.sq_dim_time_sk_id RESTART;
delete from stg.campeonatobrasileirofull;
delete from dw.f_jogos 
"""

set(query)