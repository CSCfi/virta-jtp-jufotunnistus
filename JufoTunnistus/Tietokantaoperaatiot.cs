using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Jufo_Tunnistus
{
    class Tietokantaoperaatiot
    {

        // Let's create and populate TMP-table for JufoTunnus and JufoLuokkaKoodi
        // in order to find differences between origina jufo-values and the values
        // identified by JufoTunnistus program
        public void create_and_populate_Jufot_TMP(string connection)
        {

            SqlConnection conn = new SqlConnection(connection);
            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "IF (EXISTS (SELECT * " +
                                  "FROM INFORMATION_SCHEMA.TABLES " +
                                  "WHERE TABLE_CATALOG = 'julkaisut_ods' " +
                                  "AND TABLE_SCHEMA = 'dbo' " +
                                  "AND  TABLE_NAME = 'Jufot_TMP')) " +
                              "BEGIN " +
                                  "DELETE FROM dbo.Jufot_TMP;" +
                                  "INSERT INTO dbo.Jufot_TMP " + 
                                      "SELECT JulkaisunTunnus, JufoTunnus, JufoLuokkaKoodi " + 
                                      "FROM dbo.SA_Julkaisut " +
                                      "WHERE JulkaisunTunnus NOT IN (" +
                                          "SELECT JulkaisunTunnus " +
                                          "FROM dbo.EiJufoTarkistusta);" +
                              "END";

            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;
            cmd.ExecuteNonQuery();
            
            conn.Close();

        }



        // Haetaan SA_Julkaisut-kannasta kaikki rivit
        // return SqlDataReader
        public SqlDataReader SA_julkaisut_select_sarakkeet(SqlConnection conn)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT rivi, JulkaisunTunnus, JulkaisuVuosi, JufoTunnus, JufoLuokkaKoodi, KustantajanNimi, JulkaisutyyppiKoodi, KonferenssinNimi " +
                               "FROM dbo.SA_Julkaisut " +
                               "WHERE JulkaisutyyppiKoodi IS NOT NULL " + 
                               "AND JulkaisunTunnus NOT IN (" +
                                    "SELECT JulkaisunTunnus " +
                                    "FROM dbo.EiJufoTarkistusta)";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;
        }


        // [julkaisut_ods].[dbo].[SA_ISSN]
        // [julkaisut_ods].[dbo].[SA_ISBN]
        // [julkaisut_ods].[dbo].[ODS_ISSN]
        // [julkaisut_ods].[dbo].[ODS_ISBN]
        //
        // Haetaan julkaisun ISSN- tai ISBN -tunnukset julkaisulle JulkaisunTunnus-arvon perusteella
        // Ensimmaine parametrina on tietokantayhteys ja toinen parametri kertoo mita tunnusta halutaan
        // (ISSN vai ISBN). Kolmas parametri kertoo halutaanko tunnukset SA- vai ODS -tauluista.
        public SqlDataReader hae_tunnus_julkaisulle(SqlConnection conn, string julkTunnus, string tunnus, string taulu)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            if (tunnus.Equals("ISSN") && taulu.Equals("SA"))
            {
                cmd.CommandText = "SELECT * FROM dbo.SA_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISBN") && taulu.Equals("SA"))
            {
                cmd.CommandText = "SELECT * FROM dbo.SA_ISBN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISSN") && taulu.Equals("ODS"))
            {
                cmd.CommandText = "SELECT * FROM dbo.ODS_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISBN") && taulu.Equals("ODS"))
            {
                cmd.CommandText = "SELECT * FROM dbo.ODS_ISBN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }

            cmd.CommandType = CommandType.Text;
            cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            cmd.Connection = conn;

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        // [julkaisut_ods].[dbo].[SA_ISSN]
        // [julkaisut_ods].[dbo].[SA_ISBN]
        // [julkaisut_ods].[dbo].[ODS_ISSN]
        // [julkaisut_ods].[dbo].[ODS_ISBN]
        //
        // laske parametrina annetun julkaisun ISSN- tai ISBN -tunnusten maara. Toisena parametrina annetaan tieto
        // halutaanko ISSN- vai ISBN-tunnusten maarat ja kolmas parametri kertoo, mista taulusta haetaan tieto (SA/ODS)
        public int count_tunnusten_maara(string server, string julkTunnus, string tunnus, string taulu)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            if (tunnus.Equals("ISSN") && taulu.Equals("SA"))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.SA_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISBN") && taulu.Equals("SA"))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.SA_ISBN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISSN") && taulu.Equals("ODS"))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tunnus.Equals("ISBN") && taulu.Equals("ODS"))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISBN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }

            cmd.CommandType = CommandType.Text;
            cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            cmd.Connection = conn;

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            return maara;
        }


        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Haetaan Julkaisukanavatietokanta-taulusta niiden rivien maara, joille ISSN1 = issn tai ISS2 = issn
        public int Julkaisukanavatietokanta_count_issn_match_rows(string server, string issn1, string issn2)
        {


            // jos issn1 = null tai issn2 = null, niin palautetaan -1
            if ((issn1 == null) || (issn2 == null))
            {
                return -1;
            }


            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT COUNT(*) FROM Julkaisukanavatietokanta WHERE (ISSN1 = @ekaISSN1 OR ISSN1 = @ekaISSN2 OR ISSN2 = @ekaISSN1 OR ISSN2 = @ekaISSN2) AND Active_binary <> 0";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            // issn1
            if (String.IsNullOrEmpty(issn1))
            {
                cmd.Parameters.AddWithValue("@ekaISSN1", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ekaISSN1", issn1);
            }

            // issn2
            if (String.IsNullOrEmpty(issn2))
            {
                cmd.Parameters.AddWithValue("@ekaISSN2", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ekaISSN2", issn2);
            }


            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            return maara;

        }


        //----------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Haetaan Julkaisukanavatietokanta-taulusta niiden rivien maara, joille ISBN = isbn_root
        public int Julkaisukanavatietokanta_count_isbn_root_match_rows(string server, string isbn_root)
        {

            // jos isbn = null, niin palautetaan -1
            if (isbn_root == null)
            {
                return -1;
            }

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT COUNT(*) FROM Julkaisukanavatietokanta WHERE ISBN LIKE @ISBN";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(isbn_root))
            {
                cmd.Parameters.AddWithValue("@ISBN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISBN", "%" + isbn_root + ";%");
            }

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            return maara;

        }



        public SqlDataReader Julkaisukanavatietokanta_select_name_or_other_title_kustantajan_perusteella(SqlConnection conn, string kustantaja, string valinta)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            if (valinta.Equals("name"))
            {
                cmd.CommandText = "SELECT * FROM Julkaisukanavatietokanta WHERE Name = @Kustantaja";
            }
            else if (valinta.Equals("other_title"))
            {
                cmd.CommandText = "SELECT * FROM Julkaisukanavatietokanta WHERE Other_Title = @Kustantaja";
            }

            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(kustantaja))
            {
                cmd.Parameters.AddWithValue("@Kustantaja", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@Kustantaja", kustantaja);
            }

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }





        //----------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Haetaan Julkaisukanavatietokanta-taulusta niiden rivien maara, joille name = konf
        public int Julkaisukanavatietokanta_count_konferenssi_match_rows(string server, string konf)
        {

            // jos konf = null, niin palautetaan -1
            if (konf == null)
            {
                return -1;
            }

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT COUNT(*) FROM Julkaisukanavatietokanta WHERE Active_binary = 1 AND Type = 'Konferenssi' AND (Name = @Name OR Other_Title = @Other_Title)";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(konf))
            {
                cmd.Parameters.AddWithValue("@Name", DBNull.Value);
                cmd.Parameters.AddWithValue("@Other_Title", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@Name", konf);
                cmd.Parameters.AddWithValue("@Other_Title", konf);
            }

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            return maara;

        }

        
        public SqlDataReader Julkaisukanavatietokanta_select_name(SqlConnection conn, string server)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT Jufo_ID, Name FROM dbo.Julkaisukanavatietokanta WHERE Jufo_ID IS NOT NULL";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        //----------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Haetaan ODS_ISBN-taulusta niiden rivien maara, joille ISBN = isbn1 OR ISBN = isbn2
        public int ODS_ISBN_count_isbn_match_rows(string server, string isbn1, string isbn2)
        {

            // jos isbn = null, niin palautetaan -1
            if ((isbn1 == null) || (isbn2 == null))
            {
                return -1;
            }

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            if (isbn1.Equals("") && isbn2.Equals(""))
            {
                conn.Close();
                return 0;   // parametrit ovat tyhjat, joten palautetaan 0
            }
            else if (isbn1.Equals("") && !(isbn2.Equals("")))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISBN WHERE ISBN = @ISBN";
                cmd.Parameters.AddWithValue("@ISBN", isbn2);
            }
            else if (!(isbn1.Equals("")) && isbn2.Equals(""))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISBN WHERE ISBN = @ISBN";
                cmd.Parameters.AddWithValue("@ISBN", isbn1);
            }
            else if (!(isbn1.Equals("")) && !(isbn2.Equals("")))
            {
                cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISBN WHERE ISBN = @ISBN1 OR ISBN = @ISBN2";
                cmd.Parameters.AddWithValue("@ISBN1", isbn1);
                cmd.Parameters.AddWithValue("@ISBN2", isbn2);
            }
            else
            {
                conn.Close();
                return 0;
            }
            

            //cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISBN WHERE ISBN = @ekaISBN1 OR ISBN = @ekaISBN2";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            //if (String.IsNullOrEmpty(isbn1))
            //{
            //    cmd.Parameters.AddWithValue("@ekaISBN1", DBNull.Value);
            //}
            //else
            //{
            //    cmd.Parameters.AddWithValue("@ekaISBN1", isbn1);
            //}

            //if (String.IsNullOrEmpty(isbn2))
            //{
            //    cmd.Parameters.AddWithValue("@ekaISBN2", DBNull.Value);
            //}
            //else
            //{
            //    cmd.Parameters.AddWithValue("@ekaISBN2", isbn2);
            //}
            //
            int maara = (int)cmd.ExecuteScalar();
            
            conn.Close();
            
            return maara;

        }


        // Haetaan ODS_ISBN-taulusta julkaisunTunnus sille julkaisulle, joille ISBN = isbn1 OR ISBN = isbn2
        public SqlDataReader ODS_ISBN_hae_julkaisunTunnus(SqlConnection conn, string server, string isbn1, string isbn2)
        {

            //// jos isbn1 = null tai isbn2 = null, niin palautetaan -1
            //if ((isbn1 == null) || (isbn2 == null))
            //{
            //    return "-1";
            //}

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            if (isbn1.Equals("") && isbn2.Equals(""))
            {
                cmd.CommandText = "SELECT DISTINCT JulkaisunTunnus FROM dbo.ODS_ISBN WHERE ISBN = 'ABCDEFGHIJ'";
            }
            else if (isbn1.Equals("") && !(isbn2.Equals("")))
            {
                cmd.CommandText = "SELECT DISTINCT JulkaisunTunnus FROM dbo.ODS_ISBN WHERE ISBN = @ISBN";
                cmd.Parameters.AddWithValue("@ISBN", isbn2);
            }
            else if (!(isbn1.Equals("")) && isbn2.Equals(""))
            {
                cmd.CommandText = "SELECT DISTINCT JulkaisunTunnus FROM dbo.ODS_ISBN WHERE ISBN = @ISBN";
                cmd.Parameters.AddWithValue("@ISBN", isbn1);
            }
            else if (!(isbn1.Equals("")) && !(isbn2.Equals("")))
            {
                cmd.CommandText = "SELECT DISTINCT JulkaisunTunnus FROM dbo.ODS_ISBN WHERE ISBN = @ISBN1 OR ISBN = @ISBN2";
                cmd.Parameters.AddWithValue("@ISBN1", isbn1);
                cmd.Parameters.AddWithValue("@ISBN2", isbn2);
            }
            else
            {
                cmd.CommandText = "SELECT DISTINCT JulkaisunTunnus FROM dbo.ODS_ISBN WHERE ISBN = 'ABCDEFGHIJ'";
            }

            //cmd.CommandText = "SELECT DISTINCT JulkaisunTunnus FROM dbo.ODS_ISBN WHERE ISBN = @ekaISBN1 OR ISBN = @ekaISBN2";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            //if (String.IsNullOrEmpty(isbn1))
            //{
            //    cmd.Parameters.AddWithValue("@ekaISBN1", DBNull.Value);
            //}
            //else
            //{
            //    cmd.Parameters.AddWithValue("@ekaISBN1", isbn1);
            //}

            //if (String.IsNullOrEmpty(isbn2))
            //{
            //    cmd.Parameters.AddWithValue("@ekaISBN2", DBNull.Value);
            //}
            //else
            //{
            //    cmd.Parameters.AddWithValue("@ekaISBN2", isbn2);
            //}

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        // Haetaan ODS_ISSN-taulusta niiden rivien maara, jotka matchaavat parametrina annettuun julkaisun tunnukseen.
        // Jos loytyy vahintaa yksi rivi, niin palautetaan true, muuten false
        public bool ODS_julkaisulle_loytyy_issn_tunnus(string server, string julkTunnus)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT COUNT(*) FROM dbo.ODS_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            cmd.CommandTimeout = 300;

            // JulkaisunTunnus
            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            if (maara > 0)
            {
                return true;
            }

            return false;

        }

        //----------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Paivita SA_Julkaisut-taulun JufoTunnus- ja JufoLuokkaKoodi -kentat issn:aa vastaavalle riville
        // parametrina annetuilla arvoilla
        public void SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(string server, string julkTunnus, string jufo_ID, string jufo_luokka)
        {

            // Update jufo_ID

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);
            conn.Open();

            using (conn)
            {
                SqlCommand cmd = new SqlCommand("UPDATE dbo.SA_Julkaisut SET JufoTunnus = @JufoTunnus, JufoLuokkaKoodi = @JufoLuokkaKoodi where JulkaisunTunnus = @JulkaisunTunnus");
                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;

                // Jufo_ID
                if (String.IsNullOrEmpty(jufo_ID))
                {
                    cmd.Parameters.AddWithValue("@JufoTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JufoTunnus", jufo_ID);
                }

                // Jufo-luokka koodi
                if (String.IsNullOrEmpty(jufo_luokka))
                {
                    cmd.Parameters.AddWithValue("@JufoLuokkaKoodi", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JufoLuokkaKoodi", jufo_luokka);
                }

                // JulkaisunTunnus
                if (String.IsNullOrEmpty(julkTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                }

                cmd.ExecuteNonQuery();
            }

            conn.Close();

        }


        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Haetaan Julkaisukanavatietokanta-taulusta kaikki tiedot julkaisulle ISSN-tunnuksen perusteella
        public SqlDataReader Julkaisukanavatietokanta_select_ISSN_tunnuksella(SqlConnection conn, string issn1, string issn2)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT * FROM Julkaisukanavatietokanta WHERE ISSN1 = @ekaISSN1 OR ISSN1 = @ekaISSN2 OR ISSN2 = @ekaISSN1 OR ISSN2 = @ekaISSN2";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(issn1))
            {
                cmd.Parameters.AddWithValue("@ekaISSN1", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ekaISSN1", issn1);
            }

            if (String.IsNullOrEmpty(issn2))
            {
                cmd.Parameters.AddWithValue("@ekaISSN2", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ekaISSN2", issn2);
            }

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Haetaan Julkaisukanavatietokanta-taulusta kaikki tiedot julkaisulle Name-kentan (konferenssin nimen) perusteella
        public SqlDataReader Julkaisukanavatietokanta_select_konferenssin_nimella(SqlConnection conn, string konf)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT * FROM Julkaisukanavatietokanta WHERE (Name = @Name OR Other_Title = @Other_Title) AND Active_binary = 1 AND Type = 'Konferenssi'";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(konf))
            {
                cmd.Parameters.AddWithValue("@Name", DBNull.Value);
                cmd.Parameters.AddWithValue("@Other_Title", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@Name", konf);
                cmd.Parameters.AddWithValue("@Other_Title", konf);
            }

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        public SqlDataReader Julkaisukanavatietokanta_select_jufolla(SqlConnection conn, string jufo)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT * FROM Julkaisukanavatietokanta WHERE Jufo_ID = @Jufo_ID";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(jufo))
            {
                cmd.Parameters.AddWithValue("@Jufo_ID", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@Jufo_ID", jufo);
            }

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }





        // Haetaan ODS_ISSN-taulusta parametrina annettua julkaisun tunnusta vastaava issn-tunnus
        public string ODS_ISSN_hae_issn_tunnus(string server, string julkTunnus)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT TOP 1 ISSN FROM dbo.ODS_ISSN WHERE JulkaisunTunnus = @JulkaisunTunnus";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            string issn = (string)cmd.ExecuteScalar();

            conn.Close();

            return issn;

        }


        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Haetaan Julkaisukanavatietokanta-taulusta julkaisukanavan tyyppi ISSN-tunnuksen perusteella
        public string Julkaisukanavatietokanta_select_tyyppi_ISSN_tunnuksella(string server, string issn1, string issn2)
        {


            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT TOP 1 Type FROM Julkaisukanavatietokanta WHERE ISSN1 = @ekaISSN1 OR ISSN1 = @ekaISSN2 OR ISSN2 = @ekaISSN1 OR ISSN2 = @ekaISSN2";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(issn1))
            {
                cmd.Parameters.AddWithValue("@ekaISSN1", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ekaISSN1", issn1);
            }

            if (String.IsNullOrEmpty(issn2))
            {
                cmd.Parameters.AddWithValue("@ekaISSN2", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ekaISSN2", issn2);
            }

            //if (String.IsNullOrEmpty(issn1))
            //{
            //    cmd.Parameters.AddWithValue("@ISSN1", DBNull.Value);
            //    cmd.Parameters.AddWithValue("@ISSN2", DBNull.Value);
            //}
            //else
            //{
            //    cmd.Parameters.AddWithValue("ISSN1", issn);
            //    cmd.Parameters.AddWithValue("ISSN2", issn);
            //}

            string tyyppi = (string)cmd.ExecuteScalar();

            conn.Close();

            return tyyppi;

        }


        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------




        public void SA_Julkaisut_update_ISSN(string server, string julkTunnus, string issnTunnus)
        {

            string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";

            SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);
            conn.Open();

            using (conn)
            {
                SqlCommand cmd = new SqlCommand("UPDATE SA_Julkaisut SET ISSN = @ISSN where JulkaisunTunnus = @JulkaisunTunnus");
                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;

                if (String.IsNullOrEmpty(issnTunnus))
                {
                    cmd.Parameters.AddWithValue("@ISSN", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@ISSN", issnTunnus);
                }

                if (String.IsNullOrEmpty(julkTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                }

                cmd.ExecuteNonQuery();
            }

            conn.Close();

        }


        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------------------------


        // Haetaan Julkaisukanavatietokanta-taulusta kaikki tiedot julkaisulle ISBN_Root:n perusteella
        public SqlDataReader Julkaisukanavatietokanta_select_ISBN_Root_perusteella(SqlConnection conn, string isbn_root)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT * FROM Julkaisukanavatietokanta WHERE ISBN LIKE @ISBN";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(isbn_root))
            {
                cmd.Parameters.AddWithValue("@ISBN", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@ISBN", "%" + isbn_root + ";%");
            }

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }

    }

}