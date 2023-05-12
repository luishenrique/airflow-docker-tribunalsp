

use tcesp;

create table Despesas (id smallint PRIMARY KEY IDENTITY(1,1),

                        orgao varchar(max),
                        mes varchar(max),
                        evento varchar(max),
                        nr_empenho varchar(max),
                        id_fornecedor varchar(max),
                        nm_fornecedor varchar(max),
                        dt_emissao_despesa varchar(max),
                        vl_despesa varchar(max),

                        );


select * from Municipios

drop table Despesas

