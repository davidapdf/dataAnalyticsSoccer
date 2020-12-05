CREATE SEQUENCE IF NOT EXISTS  dw.sq_dim_arena_sk_id;
CREATE TABLE IF NOT EXISTS dw.dim_arena (sk_id integer NOT NULL DEFAULT nextval('dw.sq_dim_arena_sk_id'),
						  clube  varchar(20),
						  estado  char(2),
						  arena varchar(20) NOT NULL,				 
						  dt_insert  timestamp NOT NULL DEFAULT CAST(NOW() AS TIMESTAMP),
						  dt_update  timestamp,
						  user_insert  varchar(30) NOT NULL DEFAULT current_user,
						  user_update  varchar(30),
						  fleg char(1) check(fleg = 'Y' or fleg = 'N')
						 );
ALTER SEQUENCE dw.sq_dim_arena_sk_id
OWNED BY dw.dim_arena.sk_id;
