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
            cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandTimeout = 180;
            cmd.Connection = conn;
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

        private SqlCon SqlConn;

        public Tietokantaoperaatiot() { }


        public Tietokantaoperaatiot(string connString)
        {
            SqlConn = new SqlCon(connString);
        }


        public void tyhjenna_taulu(string table)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "TRUNCATE TABLE " + table;
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public DataTable lue_tietokantataulu_datatauluun(string kysely)
        {         
        
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = kysely;

            DataTable dt = new DataTable();
            SqlDataAdapter sda = new SqlDataAdapter(SqlConn.cmd);
            sda.Fill(dt);

            SqlConn.Sulje();

            return dt;
        }


        public void kirjoita_datataulu_tietokantaan(DataTable dt, String taulu)
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


        public void uudelleenjarjesta_indeksit(string taulu)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "ALTER INDEX ALL ON " + taulu + " REORGANIZE";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void uudelleenrakenna_indeksit(string taulu)
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "ALTER INDEX ALL ON " + taulu + " REBUILD";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void paivita_ISSN_ja_ISBN_tunnukset(string taulu)
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


        public void tunnista_konferenssi()
        {
            SqlConn.Avaa();

            SqlConn.cmd.CommandText = /* @"
                WITH t AS (
                    SELECT
                         JufoTunnus
                        ,jktk.Jufo_ID
                        ,JufoLuokkaKoodi
	                    ,JufoLuokkaKoodi_e = ca.taso
                        ,JufoPaattely
                        ,rn = row_number() over (partition by t.JulkaisunTunnus order by ca.Taso desc)
                    FROM julkaisut_ods.dbo.SA_JulkaisutTMP t
                    INNER JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk on jktk.Name = t.KonferenssinNimi or jktk.Other_Title = t.KonferenssinNimi
	                CROSS APPLY (select top 1 Taso,Vuosi from julkaisut_mds.dbo.Julkaisukanavatietokanta_jufohistory jh where jh.Jufo_ID = jktk.Jufo_ID and jh.vuosi >= t.JulkaisuVuosi order by jh.Vuosi-t.JulkaisuVuosi) ca
                    WHERE t.JufoTunnus is null
                    and jktk.Active_binary = 1 
                    and jktk.[Type] = 'Konferenssi'
                    and jktk.Jufo_ID is not null
                    and (jktk.active = 'Active' or t.JulkaisuVuosi <= jktk.Year_End) 
                    and t.JulkaisutyyppiKoodi in ('A4','C2')
                UPDATE t
                SET 
	                 JufoTunnus = Jufo_ID
	                ,JufoLuokkaKoodi = JufoLuokkaKoodi_e
                    ,JufoPaattely = 'konf'
                WHERE t.rn = 1"; /*/
                @"
                WITH t AS (
               SELECT
                   Jufo_ID = coalesce(jktk5.Jufo_ID, jktk4.Jufo_ID, jktk3.Jufo_ID, jktk2.Jufo_ID, jktk1.Jufo_ID)
                   , JufoLuokkaKoodi = null
                   , JufoLuokkaKoodi_e = ca.Taso
                   , JufoPaattely = null
                   , rn = row_number() over(partition by t.JulkaisunTunnus order by ca.Taso desc)
               FROM julkaisut_ods.dbo.SA_JulkaisutTMP t
               INNER JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk1 ON jktk1.Name = t.KonferenssinNimi or jktk1.Other_Title = t.KonferenssinNimi
               -- Jos kanava ei ole toiminnassa ja sillä on jatkaja
               LEFT JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk2 ON jktk2.Jufo_ID = jktk1.Continued_by and jktk1.Continued_by is not null and not(jktk1.active = 'Active' or t.JulkaisuVuosi <= coalesce(jktk1.Year_End, 1900))
               LEFT JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk3 ON jktk3.Jufo_ID = jktk2.Continued_by and jktk2.Continued_by is not null and not(jktk2.active = 'Active' or t.JulkaisuVuosi <= coalesce(jktk2.Year_End, 1900))            
               LEFT JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk4 ON jktk4.Jufo_ID = jktk3.Continued_by and jktk3.Continued_by is not null and not(jktk3.active = 'Active' or t.JulkaisuVuosi <= coalesce(jktk3.Year_End, 1900))            
               LEFT JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk5 ON jktk5.Jufo_ID = jktk4.Continued_by and jktk4.Continued_by is not null and not(jktk4.active = 'Active' or t.JulkaisuVuosi <= coalesce(jktk4.Year_End, 1900))
               -- Joko alkuperäisen kanavan tai jatkajan jufo - taso ensimmäiseltä vuodelta joka on yhtä suuri tai suurempi kuin julkaisuvuosi
               CROSS APPLY(select top 1 Taso, Vuosi from julkaisut_mds.dbo.Julkaisukanavatietokanta_jufohistory jh where jh.Jufo_ID = coalesce(jktk5.Jufo_ID, jktk4.Jufo_ID, jktk3.Jufo_ID, jktk2.Jufo_ID, jktk1.Jufo_ID) and jh.vuosi >= t.JulkaisuVuosi order by jh.Vuosi - t.JulkaisuVuosi) ca
               WHERE t.JufoTunnus is null            
               and jktk1.Active_binary = 1            
               and jktk1.[Type] = 'Konferenssi'            
               and jktk1.Jufo_ID is not null            
               and(coalesce(jktk1.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk1.Year_End, 1900) or           
                   coalesce(jktk2.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk2.Year_End, 1900) or            
                   coalesce(jktk3.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk3.Year_End, 1900) or            
                   coalesce(jktk4.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk4.Year_End, 1900) or            
                   coalesce(jktk5.active, '') = 'Active' or t.JulkaisuVuosi <= coalesce(jktk5.Year_End, 1900)
               )            
               and t.JulkaisutyyppiKoodi in ('A4', 'C2')
               )
               select * into #konfJufoTmp from  t
               UPDATE julkaisut_ods.dbo.SA_JulkaisutTMP
               SET
                    JufoTunnus = tmp.Jufo_ID
                   , JufoLuokkaKoodi = tmp.JufoLuokkaKoodi_e
                   , JufoPaattely = 'konf'
                   FROM #konfJufoTmp tmp
               WHERE tmp.rn = 1;

               DROP TABLE #konfJufoTmp";
            /*UPDATE t
                SET
                     JufoTunnus = Jufo_ID
                    , JufoLuokkaKoodi = JufoLuokkaKoodi_e
                    , JufoPaattely = 'konf'
                WHERE t.rn = 1";*/

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


        public void tunnista_ISSN()
        {
            SqlConn.Avaa();

            // ISSN-kenttien eri kombinaatiot silmukalla
            for (int i = 1; i <= 2; i++)
            {
                for (int j = 1; j <= 2; j++)
                {
                    SqlConn.cmd.CommandText =
                        @"
                        WITH t as (
                            SELECT 
                                JufoTunnus
                                ,jktk.Jufo_ID
	                            ,JufoLuokkaKoodi
                                ,JufoLuokkaKoodi_e = ca.taso
                                    /* Jos muut julkaisutyypit otetaan mukaan tunnistukseen niin yllä oleva kannattaa muuttaa alla olevaksi
                                    case
                                        when t.JulkaisutyyppiKoodi in ('A1', 'A2', 'A3', 'A4', 'C1', 'C2') then ca.taso
                                    end
                                    */  
                                ,JufoPaattely
                                ,rn = row_number() over (partition by t.JulkaisunTunnus order by ca.Taso desc)
                            FROM julkaisut_ods.dbo.SA_JulkaisutTMP t 
                            INNER JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk on jktk.ISSN" + i + " = t.ISSN" + j + @" 
                            CROSS APPLY (select top 1 taso,Vuosi from julkaisut_mds.dbo.Julkaisukanavatietokanta_jufohistory jh 
                        where jh.Jufo_ID = jktk.Jufo_ID and jh.vuosi >= t.JulkaisuVuosi
                        order by jh.Vuosi-t.JulkaisuVuosi) ca
                            WHERE (t.JufoTunnus is null or t.JufoPaattely = 'issn')                       
                            and jktk.Active_binary = '1'
                            and (jktk.active = 'Active' or t.JulkaisuVuosi <= jktk.Year_End) 
                            and jktk.Jufo_ID is not null
                        )
                
                        UPDATE t 
                        SET 
                             JufoTunnus = Jufo_ID
	                        ,JufoLuokkaKoodi = JufoLuokkaKoodi_e
                            ,JufoPaattely = 'issn'
                        WHERE t.rn = 1 and ( t.JufoLuokkaKoodi_e IS NULL OR t.JufoLuokkaKoodi_e >= coalesce(t.JufoLuokkaKoodi,-2))";
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
                }

            }

            SqlConn.Sulje();
        }


        public void tunnista_ISBN()
        {
            SqlConn.Avaa();

            // Temp-taulun luonti
            SqlConn.cmd.CommandText = @"
                SELECT
                    JulkaisunTunnus, ISSN1 = [1], ISSN2 = [2]
                INTO ##temp_issn
                FROM (
                    SELECT
                        JulkaisunTunnus
                        , ISSN = nullif(ltrim(ISSN), '')
                        , rn = row_number() over(partition by JulkaisunTunnus order by Lataus_ID)
                    FROM julkaisut_ods.dbo.ODS_ISSN
                ) Q
                PIVOT(
                    min(ISSN) FOR rn in ([1],[2])
                ) pvt

                CREATE NONCLUSTERED INDEX [NC_temp_issn1] ON [tempdb].[dbo].[##temp_issn] ([JulkaisunTunnus]) INCLUDE ([ISSN1])
                CREATE NONCLUSTERED INDEX [NC_temp_issn2] ON [tempdb].[dbo].[##temp_issn] ([JulkaisunTunnus]) INCLUDE ([ISSN2])

                ";
            SqlConn.cmd.ExecuteNonQuery();

            // ISSN-kenttien eri kombinaatiot silmukalla
            for (int i = 1; i <= 2; i++)
            {
                for (int j = 1; j <= 2; j++)
                {
                    SqlConn.cmd.CommandText = @"
                        WITH t AS (
                            SELECT
                                JufoTunnus
                                ,jktk.Jufo_ID
	                            ,JufoLuokkaKoodi
	                            ,JufoLuokkaKoodi_e = ca.Taso
                                ,JufoPaattely
                                ,rn = row_number() over (partition by t.JulkaisunTunnus order by ca.Taso desc)
                            FROM julkaisut_ods.dbo.SA_JulkaisutTMP t 
                            INNER JOIN julkaisut_ods.dbo.ODS_ISBN i1 on (i1.ISBN = t.ISBN1 or i1.ISBN = t.ISBN2) and i1.JulkaisunTunnus != t.JulkaisunTunnus
                            INNER JOIN ##temp_issn tmp on tmp.JulkaisunTunnus = i1.JulkaisunTunnus 
                            INNER JOIN julkaisut_mds.dbo.Julkaisukanavatietokanta jktk on jktk.ISSN" + i + " = tmp.ISSN" + j + @"
                            CROSS APPLY (select top 1 Taso,Vuosi from julkaisut_mds.dbo.Julkaisukanavatietokanta_jufohistory jh where jh.Jufo_ID = jktk.Jufo_ID and jh.vuosi >= t.JulkaisuVuosi order by jh.Vuosi-t.JulkaisuVuosi) ca
                            WHERE (t.JufoTunnus is null or t.JufoPaattely = 'isbn')    
                            --and (t.JulkaisutyyppiKoodi in ('A3','C1') or (t.JulkaisutyyppiKoodi in ('A4','C2') and (t.ISSN1 is not null or t.ISSN2 is not null))) 
                            -- Jos halutaan tehdä A4 ja C2 tyypeille vastaava tarkistus kuin A3 ja C1 tyypeille niin yllä oleva korvataan alla olevalla
                            and t.JulkaisutyyppiKoodi in ('A3','C1','A4','C2')
                            and jktk.Active_binary = 1 
                            and (jktk.active = 'Active' or t.JulkaisuVuosi <= jktk.Year_End) 
                            and jktk.Jufo_ID is not null
                        )
                                   
                        UPDATE t 
                        SET 
                             JufoTunnus = Jufo_ID
	                        ,JufoLuokkaKoodi = JufoLuokkaKoodi_e
                            ,JufoPaattely = 'isbn'
                        WHERE t.rn = 1 and t.JufoLuokkaKoodi_e > coalesce(t.JufoLuokkaKoodi,-2)";

                    SqlConn.cmd.ExecuteNonQuery();
                }
            }

            // Temp-taulun poisto
            SqlConn.cmd.CommandText = "DROP TABLE ##temp_issn";
            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }

        
        public void tunnista_ISBN_juuri()
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
                and oa1_1.Item is not null

                ;WITH t AS (
                    SELECT
	                     JufoTunnus
                        ,jktk.Jufo_ID
	                    ,JufoLuokkaKoodi
	                    ,JufoLuokkaKoodi_e = ca.taso
                        ,JufoPaattely
                        ,rn = row_number() over (partition by t.JulkaisunTunnus order by ca.Taso desc)
                    FROM julkaisut_ods.dbo.SA_JulkaisutTMP t
                    OUTER APPLY (
	                    select	
		                    ISBN_juuri1 = LEFT([ISBN1],nullif(charindex('-',[ISBN1],charindex('-',[ISBN1], (charindex('-',[ISBN1])+1))+1)-1,-1))
                            -- Jos haku myös ISBN2 perusteella niin tämä mukaan
		                    ,ISBN_juuri2 = LEFT([ISBN2],nullif(charindex('-',[ISBN2],charindex('-',[ISBN2], (charindex('-',[ISBN2])+1))+1)-1,-1))
                    ) oa
                    INNER JOIN ##temp_jktk jktk on jktk.jktk_isbn = oa.ISBN_juuri1 or jktk.jktk_isbn = oa.ISBN_juuri2
	                CROSS APPLY (select top 1 Taso,Vuosi from julkaisut_mds.dbo.Julkaisukanavatietokanta_jufohistory jh where jh.Jufo_ID = jktk.Jufo_ID and jh.vuosi >= t.JulkaisuVuosi order by jh.Vuosi-t.JulkaisuVuosi) ca
                    WHERE t.JufoTunnus is null
                    and t.JulkaisutyyppiKoodi in ('A3','A4','C1','C2')
                    and (lower(t.KustantajanNimi) = lower(jktk.Name) or lower(t.KustantajanNimi) = lower(jktk.Title))     
                    and (jktk.Active = 'Active' or t.JulkaisuVuosi <= jktk.Year_End)
                )

                UPDATE t
                SET
	                 JufoTunnus = Jufo_ID
	                ,JufoLuokkaKoodi = JufoLuokkaKoodi_e
                    ,JufoPaattely = 'isbn_juuri'
                WHERE t.rn = 1

                DROP TABLE ##temp_jktk";

            SqlConn.cmd.ExecuteNonQuery();

            SqlConn.Sulje();
        }


        public void kirjoita_jufot_tmp_tauluun()
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


        public void paivita_jufot_sa_tauluun()
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = @"
                UPDATE t
                SET 
                    t.JufoTunnus = t2.JufoTunnus
                    ,t.JufoLuokkaKoodi = t2.JufoLuokkaKoodi
                FROM julkaisut_ods.dbo.SA_Julkaisut t
                INNER JOIN julkaisut_ods.dbo.SA_JulkaisutTMP t2 on t2.JulkaisunTunnus = t.JulkaisunTunnus";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }


        public void paivita_poikkeukset()
        {
            SqlConn.Avaa();
            SqlConn.cmd.CommandText = "EXEC julkaisut_ods.dbo.JufoPoikkeukset_paivitys";
            SqlConn.cmd.ExecuteNonQuery();
            SqlConn.Sulje();
        }

    }

}