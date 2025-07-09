using System;
using System.Data.SqlClient;
using System.Data;

namespace Jufo_Tunnistus
{
    class SqlCon
    {
        public SqlConnection conn;
        public SqlCommand cmd;

        public SqlCon(string connString)
        {
            conn = new SqlConnection(connString);
            cmd = new SqlCommand
            {
                CommandType = CommandType.Text,
                CommandTimeout = 180,
                Connection = conn
            };
        }

        public void Avaa()
        {
            conn.Open();
        }

        public void Sulje()
        {
            cmd.Parameters.Clear();
            conn.Close();
        }
    }


    class Tietokantaoperaatiot
    {

        private readonly SqlCon SqlConn;

        public Tietokantaoperaatiot() { }


        public Tietokantaoperaatiot(string connString)
        {
            SqlConn = new SqlCon(connString);
        }


        public void Tyhjenna_taulu(string table)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "TRUNCATE TABLE " + table;
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public DataTable Lue_tietokantataulu_datatauluun(string kysely)
        {

            SqlConn.Avaa();
            SqlConn.cmd.CommandText = kysely;

            DataTable dt = new DataTable();
            SqlDataAdapter sda = new SqlDataAdapter(SqlConn.cmd);
            sda.Fill(dt);

            SqlConn.Sulje();

            return dt;
        }


        public void Kirjoita_datataulu_tietokantaan(DataTable dt, String taulu)
        {
            SqlConn.Avaa();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(SqlConn.conn))
            {
                // Sarakkeiden mappaus datataulun ja tietokantataulun välillä
                foreach (DataColumn column in dt.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                bulkCopy.BatchSize = 10000;
                bulkCopy.DestinationTableName = taulu;

                try
                {
                    bulkCopy.WriteToServer(dt);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
            }

            SqlConn.Sulje();
        }


        public void Uudelleenjarjesta_indeksit(string taulu)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "ALTER INDEX ALL ON " + taulu + " REORGANIZE";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Uudelleenrakenna_indeksit(string taulu)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "ALTER INDEX ALL ON " + taulu + " REBUILD";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Paivita_ISSN_ja_ISBN_tunnukset(string taulu)
        {
            SqlConn.Avaa();

            // ISSN
            SqlConn.cmd.CommandText = @"
                UPDATE j
                SET
                     j.ISSN1 = d2.[1]
                    ,j.ISSN2 = d2.[2]
                FROM " + taulu + @" j
                LEFT JOIN (
                    SELECT
                        JulkaisunTunnus,[1],[2]
                    FROM (
                        SELECT
                            JulkaisunTunnus
                            , ISSN = nullif(ltrim(ISSN), '')
                            , rn = row_number() over(partition by JulkaisunTunnus order by Lataus_ID)
                        FROM [julkaisut_ods].[dbo].[SA_ISSN]
	                ) Q
                    PIVOT(
                        min(ISSN) FOR rn in ([1],[2])
	                ) pvt
                ) d2 on d2.JulkaisunTunnus = j.JulkaisunTunnus";
            SqlConn.cmd.ExecuteNonQuery();

            // ISBN
            SqlConn.cmd.CommandText = @"
                UPDATE j
                SET
                     j.ISBN1 = d2.[1]
                    ,j.ISBN2 = d2.[2]
                FROM " + taulu + @" j
                LEFT JOIN (
                    SELECT
                        JulkaisunTunnus,[1],[2]
                    FROM (
                        SELECT
                            JulkaisunTunnus
                            , ISBN = nullif(ltrim(ISBN), '')
                            , rn = row_number() over(partition by JulkaisunTunnus order by Lataus_ID)
                        FROM [julkaisut_ods].[dbo].[SA_ISBN]
	                ) Q
                    PIVOT(
                        min(ISBN) FOR rn in ([1],[2])
	                ) pvt
                ) d2 on d2.JulkaisunTunnus = j.JulkaisunTunnus";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Tunnista_konferenssi(string taulu_julkaisut, string taulu_jufot)
        {
            SqlConn.Avaa();

            SqlConn.cmd.CommandText = @"
                INSERT INTO " + taulu_jufot + @" (JulkaisunTunnus,Julkaisuvuosi,Jufo_ID,JufoPaattely,JulkaisutyyppiKoodi)
                SELECT
                     t.JulkaisunTunnus
	                ,t.JulkaisuVuosi
                    ,jktk.Jufo_ID
                    ,JufoPaattely = 'konf'
                    ,t.JulkaisutyyppiKoodi
                FROM " + taulu_julkaisut + @" t
                INNER JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk on jktk.Name COLLATE Latin1_General_CI_AI = t.KonferenssinNimi COLLATE Latin1_General_CI_AI
                    or jktk.Other_Title COLLATE Latin1_General_CI_AI = t.KonferenssinNimi COLLATE Latin1_General_CI_AI
                WHERE NOT EXISTS (select 1 from " + taulu_jufot + @" where JulkaisunTunnus=t.JulkaisunTunnus)
                and jktk.Active_binary = 1 
                and jktk.[Type] = 'Konferenssi'
                and jktk.Jufo_ID is not null
                and t.JulkaisutyyppiKoodi in ('A4','C2','D3','D6')";

            try
            {
                // Execute your SQL command    
                SqlConn.cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                // Handle SQL exceptions
                Console.WriteLine("SQL Error: " + ex.Message);
            }

            SqlConn.Sulje();
        }


        public void Tunnista_ISSN(string taulu_julkaisut, string taulu_jufot)
        {
            SqlConn.Avaa();

            // ISSN-kenttien eri kombinaatiot silmukalla
            for (int i = 1; i <= 2; i++)
            {
                for (int j = 1; j <= 2; j++)
                {
                    SqlConn.cmd.CommandText =
                        @"
                        INSERT INTO " + taulu_jufot + @" (JulkaisunTunnus,Julkaisuvuosi,Jufo_ID,JufoPaattely,JulkaisutyyppiKoodi)
                        SELECT
                             t.JulkaisunTunnus
	                        ,t.JulkaisuVuosi
                            ,jktk.Jufo_ID
                            ,JufoPaattely = 'issn'
                            ,t.JulkaisutyyppiKoodi
                        FROM " + taulu_julkaisut + @" t
                        INNER JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk on jktk.ISSN" + i + " = t.ISSN" + j + @"
                        WHERE NOT EXISTS (select 1 from " + taulu_jufot + @" where JulkaisunTunnus=t.JulkaisunTunnus and JufoPaattely != 'issn')                         
                        and jktk.Active_binary = 1
                        and jktk.Jufo_ID is not null";

                    try
                    {
                        // Execute your SQL command
                        SqlConn.cmd.CommandTimeout = 180;
                        SqlConn.cmd.ExecuteNonQuery();
                    }
                    catch (SqlException ex)
                    {
                        // Handle SQL exceptions
                        Console.WriteLine("SQL Error: " + ex.Message);
                    }
                }

            }

            SqlConn.Sulje();
        }


        public void Tunnista_ISBN(string taulu_julkaisut, string taulu_jufot)
        {
            SqlConn.Avaa();

            // Temp-taulun luonti
            // Rajataan pois -1 ja 0 tilaiset julkaisut
            SqlConn.cmd.CommandText = @"
                SELECT
                    JulkaisunTunnus, ISSN1 = [1], ISSN2 = [2]
                INTO ##temp_issn
                FROM (
                    SELECT
                        i.JulkaisunTunnus
                        ,ISSN = nullif(ltrim(i.ISSN), '')
                        ,rn = row_number() over(partition by i.JulkaisunTunnus order by i.Lataus_ID)
                    FROM julkaisut_ods.dbo.ODS_ISSN i
                    LEFT JOIN julkaisut_mds.koodi.julkaisuntunnus j ON j.JulkaisunTunnus = i.JulkaisunTunnus
                    WHERE j.JulkaisunTila between 1 and 8
                ) Q
                PIVOT(
                    min(ISSN) FOR rn in ([1],[2])
                ) pvt

                CREATE NONCLUSTERED INDEX [NC_temp_issn1] ON [tempdb].[dbo].[##temp_issn] ([JulkaisunTunnus]) INCLUDE ([ISSN1])
                CREATE NONCLUSTERED INDEX [NC_temp_issn2] ON [tempdb].[dbo].[##temp_issn] ([JulkaisunTunnus]) INCLUDE ([ISSN2])
            ";
            SqlConn.cmd.ExecuteNonQuery();

            // ISBN-kenttien eri kombinaatiot silmukalla
            for (int i = 1; i <= 2; i++)
            {
                for (int j = 1; j <= 2; j++)
                {
                    SqlConn.cmd.CommandText = @"
                        INSERT INTO " + taulu_jufot + @" (JulkaisunTunnus,Julkaisuvuosi,Jufo_ID,JufoPaattely,JulkaisutyyppiKoodi)
                        SELECT distinct
                             t.JulkaisunTunnus
	                        ,t.JulkaisuVuosi
                            ,jktk.Jufo_ID
                            ,JufoPaattely = 'isbn'
                            ,t.JulkaisutyyppiKoodi
                        FROM " + taulu_julkaisut + @" t
                        INNER JOIN julkaisut_ods.dbo.ODS_ISBN i1 on (i1.ISBN = t.ISBN1 or i1.ISBN = t.ISBN2) and i1.JulkaisunTunnus != t.JulkaisunTunnus
                        INNER JOIN ##temp_issn tmp on tmp.JulkaisunTunnus = i1.JulkaisunTunnus 
                        INNER JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk on jktk.ISSN" + i + " = tmp.ISSN" + j + @"
                        WHERE NOT EXISTS (select 1 from " + taulu_jufot + @" where JulkaisunTunnus=t.JulkaisunTunnus and JufoPaattely != 'isbn')                         
                        and t.JulkaisutyyppiKoodi in ('A3','A4','C1','C2','D2','D3','D5','D6','E2')
                        and jktk.Active_binary = 1
                        and jktk.Jufo_ID is not null
                        ";
                    SqlConn.cmd.CommandTimeout = 180;
                    SqlConn.cmd.ExecuteNonQuery();
                }
            }

            // Temp-taulun poisto
            SqlConn.cmd.CommandText = "DROP TABLE ##temp_issn";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Tunnista_ISBN_juuri(string taulu_julkaisut, string taulu_jufot)
        {
            SqlConn.Avaa();

            SqlConn.cmd.CommandText = @"
                CREATE TABLE ##temp_jktk (
	                jktk_isbn varchar(100),
	                Title varchar(500),
	                Name varchar(500),
	                Year_End int,
	                Active varchar(20),
	                Jufo_ID bigint,
	                Jufo_History varchar(500)
                )

                INSERT INTO ##temp_jktk (jktk_isbn,Title,Name,Year_End,Active,Jufo_ID,Jufo_History)

                SELECT 
	                jktk_isbn = ltrim(rtrim(oa1.Item))
	                ,Title = replace(ltrim(rtrim(oa2.item)),' & ',' and ')
	                ,[Name] = replace(jktk.[Name],' & ',' and ')
	                ,Year_End
	                ,Active
	                ,Jufo_ID
	                ,Jufo_History
                FROM julkaisut_mds.dbo.Julkaisukanavatietokanta jktk
                -- ISBN splittaus kahdessa osassa koska osassa kentistä erottimena on puolipiste ja osassa pilkku
                OUTER APPLY (select item from julkaisut_ods.dbo.SplitStrings(jktk.ISBN,';')) oa1_1
                OUTER APPLY (select item from julkaisut_ods.dbo.SplitStrings(oa1_1.Item,',')) oa1
                OUTER APPLY (select item from julkaisut_ods.dbo.SplitStrings(jktk.Other_Title,';')) oa2
                WHERE [Type] = 'Kirjakustantaja' 
                and jktk.Jufo_ID is not null
                and jktk.Active_binary = 1
                and oa1_1.Item is not null;

                INSERT INTO " + taulu_jufot + @" (JulkaisunTunnus,Julkaisuvuosi,Jufo_ID,JufoPaattely,JulkaisutyyppiKoodi)

                SELECT distinct
                     t.JulkaisunTunnus
	                ,t.JulkaisuVuosi
                    ,jktk.Jufo_ID
                    ,JufoPaattely = 'isbn_juuri'
                    ,t.JulkaisutyyppiKoodi
                FROM " + taulu_julkaisut + @" t
                OUTER APPLY (
	                select	
		                ISBN_juuri1 = LEFT([ISBN1],nullif(charindex('-',[ISBN1],charindex('-',[ISBN1], (charindex('-',[ISBN1])+1))+1)-1,-1))
		                ,ISBN_juuri2 = LEFT([ISBN2],nullif(charindex('-',[ISBN2],charindex('-',[ISBN2], (charindex('-',[ISBN2])+1))+1)-1,-1))
                ) oa
                INNER JOIN ##temp_jktk jktk on jktk.jktk_isbn = oa.ISBN_juuri1 or jktk.jktk_isbn = oa.ISBN_juuri2     
                WHERE NOT EXISTS (select 1 from " + taulu_jufot + @" where JulkaisunTunnus=t.JulkaisunTunnus)
                and t.JulkaisutyyppiKoodi in ('A3','A4','C1','C2','D2','D3','D5','D6','E2')
                and (
                    t.KustantajanNimi COLLATE Latin1_General_CI_AI = jktk.Name COLLATE Latin1_General_CI_AI 
                    or t.KustantajanNimi COLLATE Latin1_General_CI_AI = jktk.Title COLLATE Latin1_General_CI_AI
                )

                DROP TABLE ##temp_jktk";

            SqlConn.cmd.CommandTimeout = 180;
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void Hae_virta_additions(string taulu_julkaisut, string taulu_jufot)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = @"
                INSERT INTO " + taulu_jufot + @" (JulkaisunTunnus, Julkaisuvuosi, Jufo_ID, JufoPaattely, JulkaisutyyppiKoodi)
                SELECT
                     t.JulkaisunTunnus
	                ,t.JulkaisuVuosi
                    ,v.Jufo_ID
                    ,JufoPaattely = 'additions'
                    ,t.JulkaisutyyppiKoodi
                FROM " + taulu_julkaisut + @" t
                INNER JOIN julkaisut_mds.dbo.VirtaAdditions v ON v.julkaisuntunnus = t.JulkaisunTunnus
                WHERE NOT EXISTS (select 1 from " + taulu_jufot + @" where JulkaisunTunnus=t.JulkaisunTunnus)
                AND t.JulkaisutyyppiKoodi IN ('A1','A2','A3','A4','C1','C2')
            ";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Hae_kanavan_jatkajat(string taulu_jufot)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = @"
                UPDATE t
                SET
	                t.Jufo_ID_actual =  coalesce(jktk5.Jufo_ID, jktk4.Jufo_ID, jktk3.Jufo_ID, jktk2.Jufo_ID, jktk1.Jufo_ID)
                FROM " + taulu_jufot + @" t
                INNER JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk1 ON jktk1.Jufo_ID = t.Jufo_ID 
                LEFT JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk2 ON jktk2.Jufo_ID = jktk1.Continued_by and jktk1.Continued_by is not null and not(jktk1.active = 'Active' or t.JulkaisuVuosi <= coalesce(jktk1.Year_End, 1900))
                LEFT JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk3 ON jktk3.Jufo_ID = jktk2.Continued_by and jktk2.Continued_by is not null and not(jktk2.active = 'Active' or t.JulkaisuVuosi <= coalesce(jktk2.Year_End, 1900))
                LEFT JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk4 ON jktk4.Jufo_ID = jktk3.Continued_by and jktk3.Continued_by is not null and not(jktk3.active = 'Active' or t.JulkaisuVuosi <= coalesce(jktk3.Year_End, 1900))
                LEFT JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk5 ON jktk5.Jufo_ID = jktk4.Continued_by and jktk4.Continued_by is not null and not(jktk4.active = 'Active' or t.JulkaisuVuosi <= coalesce(jktk4.Year_End, 1900))
                WHERE 1=1
                and t.Jufo_ID_actual is null
                and (
                    coalesce(jktk1.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk1.Year_End, 1900) or
                    coalesce(jktk2.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk2.Year_End, 1900) or
                    coalesce(jktk3.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk3.Year_End, 1900) or
                    coalesce(jktk4.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk4.Year_End, 1900) or
                    coalesce(jktk5.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk5.Year_End, 1900)
                )";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Poista_Inaktiiviset(string taulu_jufot)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "DELETE FROM " + taulu_jufot + @" WHERE Jufo_ID_actual is null";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Hae_jufo_tasot(string taulu_jufot)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = @"
                UPDATE t
                SET t.JufoLuokka = ca.Vuosi_Jufo_Luokka
                FROM " + taulu_jufot + @" t
                CROSS APPLY (
                    select top 1 jh.Vuosi_Jufo_Luokka 
                    from julkaisut_mds.dbo.jufoluokat_vuosittain jh 
                    where jh.Jufo_ID = t.Jufo_ID_actual and jh.vuosi >= t.JulkaisuVuosi 
                    order by jh.vuosi-t.JulkaisuVuosi
                ) ca
            ";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Paivita_poikkeukset()
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "EXEC julkaisut_ods.dbo.JufoPoikkeukset_paivitys";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Rankkaa_jufo_kanavat(string taulu_jufot)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = @"
                WITH t as (
                    SELECT 
                        JufoLuokka_Rank
                        ,jl_rank = row_number() over (partition by JulkaisunTunnus order by JufoLuokka desc, id)
                    FROM " + taulu_jufot + @"
                )
                UPDATE t
                SET JufoLuokka_Rank = jl_rank
            ";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Kirjoita_alkuperaiset_jufot_tmp_tauluun()
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = @"
                TRUNCATE TABLE julkaisut_ods.dbo.Jufot_TMP
                INSERT INTO julkaisut_ods.dbo.Jufot_TMP (JulkaisunTunnus, JufoTunnus, JufoLuokkaKoodi)
                SELECT JulkaisunTunnus, JufoTunnus, JufoLuokkaKoodi 
                FROM julkaisut_ods.dbo.SA_Julkaisut";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Paivita_jufot_julkaisut_temp_tauluun(string taulu_julkaisut, string taulu_jufot)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = @"
                UPDATE t
                SET 
                    t.JufoTunnus = t2.Jufo_ID_actual
                    ,t.JufoLuokkaKoodi = t2.JufoLuokka
                    ,t.JufoPaattely = t2.JufoPaattely
                FROM " + taulu_julkaisut + @" t
                INNER JOIN " + taulu_jufot + @" t2 on t2.JulkaisunTunnus = t.JulkaisunTunnus
                WHERE t2.JufoLuokka_Rank=1";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void Paivita_jufot_sa_tauluun(string taulu_julkaisut_temp)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = @"
                UPDATE t
                SET 
                    t.JufoTunnus = t2.JufoTunnus
                    ,t.JufoLuokkaKoodi = t2.JufoLuokkaKoodi
                FROM julkaisut_ods.dbo.SA_Julkaisut t
                INNER JOIN " + taulu_julkaisut_temp + " t2 on t2.JulkaisunTunnus = t.JulkaisunTunnus";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }

    }

}