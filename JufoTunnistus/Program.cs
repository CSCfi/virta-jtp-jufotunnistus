using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Jufo_Tunnistus
{
    class Program
    {
        static void Main(string[] args)
        {

            //Console.Write("Alussa ollaan");

            if (args.Length != 1)
            {
                Console.Write("Argumenttien maara on vaara.");
            }
            else
            {

                Apufunktiot apufunktiot = new Apufunktiot();

                Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot();

                // Haetaan SA_Julkaisut-taulusta kaikilta riveilta seuraavat sarakkeet:
                // --------------------------------------------------------------------
                // rivi  
                // JulkaisunTunnus                                                  
                // JulkaisuVuosi                                                        
                // ISSN                                                                 
                // JufoTunnus
                // JufoLuokkaKoodi
                // ISBN
                // KustantajanNimi
                // JulkaisutyyppiKoodi
                // KonferenssinNimi
                //---------------------------------------------------------------------

                string server = args[0];

                string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
                string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

                // create and populate table that contains the original jufo-values
                tietokantaoperaatiot.create_and_populate_Jufot_TMP(connectionString_ods_julkaisut);

                SqlConnection conn_SA = new SqlConnection(connectionString_ods_julkaisut);
                SqlDataReader reader_SA = tietokantaoperaatiot.SA_julkaisut_select_sarakkeet(conn_SA);


                while (reader_SA.Read())
                {

                    long ekaRivi = (long)reader_SA["rivi"];
                    string ekaJulkaisunTunnus = reader_SA["JulkaisunTunnus"] == System.DBNull.Value ? null : (string)reader_SA["JulkaisunTunnus"];
                    int ekaJulkaisuvuosi = (int)reader_SA["JulkaisuVuosi"];
                    string ekaJufo_Id = reader_SA["JufoTunnus"] == System.DBNull.Value ? null : (string)reader_SA["JufoTunnus"];
                    string ekaJufoLuokka = reader_SA["JufoLuokkaKoodi"] == System.DBNull.Value ? null : (string)reader_SA["JufoLuokkaKoodi"];
                    string ekaKonferenssi = reader_SA["KonferenssinNimi"] == System.DBNull.Value ? null : (string)reader_SA["KonferenssinNimi"];
                    string ekaKustantaja = reader_SA["KustantajanNimi"] == System.DBNull.Value ? null : (string)reader_SA["KustantajanNimi"];

                    // Muokataan konferenssin nimea ja kustantajaa mikali niissa on stop wordseja tai stop charseja
                    if ((ekaKonferenssi != null) && !(ekaKonferenssi.Equals("")))
                    {
                        ekaKonferenssi = apufunktiot.muokkaa_nimea(ekaKonferenssi);
                    }

                    if ((ekaKustantaja != null) && !(ekaKustantaja.Equals("")))
                    {
                        ekaKustantaja = apufunktiot.muokkaa_nimea(ekaKustantaja);
                    }

                    string ekaJulkaisutyyppi = reader_SA["JulkaisutyyppiKoodi"] == System.DBNull.Value ? null : (string)reader_SA["JulkaisutyyppiKoodi"];



                    // Trimmataan julkaisutyyppi
                    ekaJulkaisutyyppi = ekaJulkaisutyyppi.Trim();

                    // ISSN- ja ISBN -tunnukset. Naiden arvot haetaan kohta
                    string ekaISSN1 = "";
                    string ekaISSN2 = "";

                    string ekaISBN1 = "";
                    string ekaISBN2 = "";


                    // ISSN- ja ISBN -tiedot eivät mene SA_Julkaisut -tauluun vaan omiin tauluihin, joka ovat
                    // SA_ISSN ja SA_ISBN. Haetaan ISSN- ja ISBN -tiedot kyseísista tauluista JulkaisunTunnus-arvon perusteella
                    // Parametrina on julkaisunTunnus seka tietokantayhteys
                    // Tassa haetaan SA_Julkaisut-taulun ISSN -ja ISBN-tunnukset (vahan myohemmin haetaan ODS_Julkaisut-taulun vastaavat)

                    SqlConnection conn_SA_ISSN = new SqlConnection(connectionString_ods_julkaisut);
                    SqlConnection conn_SA_ISBN = new SqlConnection(connectionString_ods_julkaisut);

                    SqlDataReader reader_SA_ISSN = tietokantaoperaatiot.hae_tunnus_julkaisulle(conn_SA_ISSN, ekaJulkaisunTunnus, "ISSN", "SA");
                    SqlDataReader reader_SA_ISBN = tietokantaoperaatiot.hae_tunnus_julkaisulle(conn_SA_ISBN, ekaJulkaisunTunnus, "ISBN", "SA");

                    int ISSN_tunnusten_maara_SA = tietokantaoperaatiot.count_tunnusten_maara(server, ekaJulkaisunTunnus, "ISSN", "SA");
                    int ISBN_tunnusten_maara_SA = tietokantaoperaatiot.count_tunnusten_maara(server, ekaJulkaisunTunnus, "ISBN", "SA");

                    int laskuri_ISSN = 0;
                    int laskuri_ISBN = 0;

                    // asetetaan issn1 -ja issn2 -tunnukset
                    while (reader_SA_ISSN.Read())
                    {
                        laskuri_ISSN = laskuri_ISSN + 1;

                        if (laskuri_ISSN == 1)
                        {

                            ekaISSN1 = reader_SA_ISSN["ISSN"] == System.DBNull.Value ? null : (string)reader_SA_ISSN["ISSN"];

                            if (ISSN_tunnusten_maara_SA == 1)
                            {
                                break;
                            }

                        }
                        else if (laskuri_ISSN == 2)
                        {

                            ekaISSN2 = reader_SA_ISSN["ISSN"] == System.DBNull.Value ? null : (string)reader_SA_ISSN["ISSN"];
                            break;

                        }
                        else
                        {
                            break;
                        }
                    }


                    // asetetaan isbn1 -ja isbn2 -tunnukset
                    while (reader_SA_ISBN.Read())
                    {
                        laskuri_ISBN = laskuri_ISBN + 1;

                        if (laskuri_ISBN == 1)
                        {

                            ekaISBN1 = reader_SA_ISBN["ISBN"] == System.DBNull.Value ? null : (string)reader_SA_ISBN["ISBN"];

                            if (ISBN_tunnusten_maara_SA == 1)
                            {
                                break;
                            }

                        }
                        else if (laskuri_ISBN == 2)
                        {

                            ekaISBN2 = reader_SA_ISBN["ISBN"] == System.DBNull.Value ? null : (string)reader_SA_ISBN["ISBN"];
                            break;

                        }
                        else
                        {
                            break;
                        }
                    }


                    // trimmataan ISBN-tunnukset ja korvataan tyhjat merkit valiviivoilla
                    ekaISBN1 = ekaISBN1.Trim().Replace(" ", "-");
                    ekaISBN2 = ekaISBN2.Trim().Replace(" ", "-");

                    reader_SA_ISSN.Close();
                    reader_SA_ISBN.Close();

                    conn_SA_ISSN.Close();
                    conn_SA_ISBN.Close();


                    // Mikali JufoTunnus != null, niin asetetaan JufoTunnus = null. Samoin tehdaan JufoLuokkaKoodille
                    if (ekaJufo_Id != null)
                    {
                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, null, null);
                    }


                    // Haetaan Julkaisukanavatietokanta-taulusta niiden rivien maara, joille ISSN1 = ekaISSN1, ISSN1 = ekaISSN2, ISSN2 = ekaISSN1 tai ISSN2 = ekaISSN2
                    int count_ISSN_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_issn_match_rows(server, ekaISSN1, ekaISSN2);                  

                    // Haetaan Julkaisukanavatietokanta-taulusta niiden rivien maara, joille Name = ekaKonferenssi tai Other_Title = ekaKonferenssi
                    int count_konferenssi_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_konferenssi_match_rows(server, ekaKonferenssi);
                                    
  
                    // Haetaan ODS_Julkaisut-taulusta niiden rivien maara, joille ISBN = ekaISBN1 tai ISBN = ekaISBN2
                    int count_ISBN_match_rows = tietokantaoperaatiot.ODS_ISBN_count_isbn_match_rows(server, ekaISBN1, ekaISBN2);


                    // Tutkitaan ensin tapaus, jossa julkaisutyyppi on joko A1 tai A2
                    if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2"))
                    {

                        // Jos count_ISSN_match_rows > 0, niin loytyy ISSN-matcheja
                        if (count_ISSN_match_rows > 0)
                        {

                            apufunktiot.jufo_tarkistus_issn_tunnuksella(server, ekaISSN1, ekaISSN2, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);

                        }

                    }


                    // Tutkitaan sitten tapaus, jossa julkaisutyyppi on A3 tai C1
                    else if (ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("C1"))
                    {

                        // tarkistetaan onko julkaisulle ilmoitettu ISSN-tunnus
                        // Jos mennaan tahan haaraan, niin ISSN-tunnus on annettu
                        if (ISSN_tunnusten_maara_SA > 0)
                        {

                            // Jos ISSN-matcheja loytyy, niin mennaan tahan silmukkaan
                            if (count_ISSN_match_rows > 0)
                            {

                                // tehdaan jufo-tarkistus ISSN-tunnuksen perusteella
                                apufunktiot.jufo_tarkistus_issn_tunnuksella(server, ekaISSN1, ekaISSN2, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                goto ENDLOOP;

                            }

                            // jos ISSN-matcheja ei loydy, niin mennaan tahan silmukkaan
                            else
                            {

                                // jos julkaisun ISBN-tunnuksella loytyy muita julkaisuja ODS_Julkaisut -taulusta, niin mennaan tanne
                                if (count_ISBN_match_rows > 0)
                                {

                                    SqlConnection conn_eka = new SqlConnection(connectionString_ods_julkaisut);
                                    SqlDataReader reader_eka = tietokantaoperaatiot.ODS_ISBN_hae_julkaisunTunnus(conn_eka, server, ekaISBN1, ekaISBN2);

                                    while (reader_eka.Read())
                                    {

                                        string ODS_julkTunnus = reader_eka["JulkaisunTunnus"] == System.DBNull.Value ? null : (string)reader_eka["JulkaisunTunnus"];

                                        // tutkitaan loytyyko kyseiselle julkaisunTunnukselle ISSN-tunnusta ODS-alueelta
                                        bool ODS_julkaisulle_loytyy_issn_tunnus = tietokantaoperaatiot.ODS_julkaisulle_loytyy_issn_tunnus(server, ODS_julkTunnus);

                                        if (ODS_julkaisulle_loytyy_issn_tunnus) // julkaisulle loytyy ISSN-tunnus
                                        {

                                            // Haetaan kyseinen ISSN-tunnus
                                            string ODS_ISSN_tunnus = tietokantaoperaatiot.ODS_ISSN_hae_issn_tunnus(server, ODS_julkTunnus);

                                            // Tutkitaan sitten loytyyko julkaisukanavatietokannasta riveja kyseisella ISSN-tunnuksella
                                            int julkaisukanavatietokanta_count_rows_based_on_ODS_ISSN = tietokantaoperaatiot.Julkaisukanavatietokanta_count_issn_match_rows(server, ODS_ISSN_tunnus, "");

                                            // Jos julkaisukanavatietokannasta loytyy ODS_Julkaisut -taulun ISSN-tunnusta vastaava match, niin mennaan tahan haaraan
                                            if (julkaisukanavatietokanta_count_rows_based_on_ODS_ISSN > 0)
                                            {

                                                apufunktiot.jufo_tarkistus_issn_tunnuksella(server, ODS_ISSN_tunnus, "", ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                goto ENDLOOP;

                                            }

                                            // Jos julkaisukanavatietokannasta ei loydy ODS_Julkaisut -taulun ISSN-tunnusta vastaavaa matchia, niin mennaan tahan haaraan
                                            else
                                            {

                                                // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                                // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                                // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                                string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                                // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                                if (!(juuri.Equals("-1")))
                                                {

                                                    // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                                    int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                                    // Jos loytyy yksi tai useampi rivi, joilla on ISBN-juuri, joka vastaa SA_Julkaisut-taulun ISBN-juurta, niin mennaan tahan silmukkaan
                                                    if (count_isbn_root_match_rows > 0)
                                                    {

                                                        apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                        goto ENDLOOP;

                                                    }

                                                }

                                            }

                                        }


                                        else if (ODS_julkaisulle_loytyy_issn_tunnus == false)   // ei loydy ISSN-tunnusta
                                        {

                                            // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                            // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                            // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                            string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                            // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                            if (!(juuri.Equals("-1")))
                                            {

                                                // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                                int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                                // Jos loytyy yksi tai useampi rivi, joilla on ISBN-juuri, joka vastaa SA_Julkaisut-taulun ISBN-juurta, niin mennaan tahan silmukkaan
                                                if (count_isbn_root_match_rows > 0)
                                                {

                                                    apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                    goto ENDLOOP;

                                                }

                                            }

                                        }

                                    }

                                    reader_eka.Close();
                                    conn_eka.Close();

                                }


                                // Jos samalla ISBN-tunnuksella ei loydy muita julkaisuja ODS_Julkaisut-taulusta, niin mennaan tanne
                                else if (count_ISBN_match_rows <= 0)
                                {

                                    // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                    // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                    // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                    string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                    // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                    if (!(juuri.Equals("-1")))
                                    {

                                        // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                        int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                        // Jos julkaisukanavatietokannasta loytyy yksi tai useampi rivi, jolle ISBN-juuri matchaa, niin haetaan nama julkaisukanavat 
                                        // ja tarkistetaan tasmaavatko myos julkaisussa oleva kustantajan nimi julkaisukanavatietokannassa oleviin Name- tai Other_Title -kenttiin
                                        if (count_isbn_root_match_rows > 0)
                                        {

                                            apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                            goto ENDLOOP;

                                        }

                                    }

                                }

                            }

                        }


                        // Jos mennaan tahan haaraan, niin julkaisulle ei ole annettu ISSN-tunnusta
                        else if (ISSN_tunnusten_maara_SA <= 0)
                        {

                            // jos samalla ISBN-tunnuksella loytyy muita julkaisuja ODS_Julkaisut -taulusta, niin mennaan tanne
                            if (count_ISBN_match_rows > 0)
                            {

                                SqlConnection conn_eka = new SqlConnection(connectionString_ods_julkaisut);
                                SqlDataReader reader_eka = tietokantaoperaatiot.ODS_ISBN_hae_julkaisunTunnus(conn_eka, server, ekaISBN1, ekaISBN2);

                                while (reader_eka.Read())
                                {

                                    string ODS_julkTunnus = reader_eka["JulkaisunTunnus"] == System.DBNull.Value ? null : (string)reader_eka["JulkaisunTunnus"];

                                    // tutkitaan loytyyko kyseiselle julkaisunTunnukselle ISSN-tunnusta ODS-alueelta
                                    bool ODS_julkaisulle_loytyy_issn_tunnus = tietokantaoperaatiot.ODS_julkaisulle_loytyy_issn_tunnus(server, ODS_julkTunnus);

                                    if (ODS_julkaisulle_loytyy_issn_tunnus) // julkaisulle loytyy ISSN-tunnus
                                    {

                                        // Haetaan kyseinen ISSN-tunnus
                                        string ODS_ISSN_tunnus = tietokantaoperaatiot.ODS_ISSN_hae_issn_tunnus(server, ODS_julkTunnus);

                                        // Tutkitaan sitten loytyyko julkaisukanavatietokannasta riveja kyseisella ISSN-tunnuksella
                                        int julkaisukanavatietokanta_count_rows_based_on_ODS_ISSN = tietokantaoperaatiot.Julkaisukanavatietokanta_count_issn_match_rows(server, ODS_ISSN_tunnus, "");

                                        // Jos julkaisukanavatietokannasta loytyy ODS_Julkaisut -taulun ISSN-tunnusta vastaava match, niin mennaan tahan haaraan
                                        if (julkaisukanavatietokanta_count_rows_based_on_ODS_ISSN > 0)
                                        {

                                            apufunktiot.jufo_tarkistus_issn_tunnuksella(server, ODS_ISSN_tunnus, "", ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                            goto ENDLOOP;

                                        }

                                        // Jos julkaisukanavatietokannasta ei loydy ODS_Julkaisut -taulun ISSN-tunnusta vastaavaa matchia, niin mennaan tahan haaraan
                                        else
                                        {

                                            // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                            // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                            // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                            string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                            // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                            if (!(juuri.Equals("-1")))
                                            {

                                                // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                                int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                                // Jos loytyy yksi tai useampi rivi, joilla on ISBN-juuri, joka vastaa SA_Julkaisut-taulun ISBN-juurta, niin mennaan tahan silmukkaan
                                                if (count_isbn_root_match_rows > 0)
                                                {

                                                    apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                    goto ENDLOOP;

                                                }

                                            }

                                        }

                                    }


                                    else if (ODS_julkaisulle_loytyy_issn_tunnus == false)   // ei loydy ISSN-tunnusta
                                    {

                                        // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                        // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                        // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                        string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                        // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                        if (!(juuri.Equals("-1")))
                                        {

                                            // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                            int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                            // Jos loytyy yksi tai useampi rivi, joilla on ISBN-juuri, joka vastaa SA_Julkaisut-taulun ISBN-juurta, niin mennaan tahan silmukkaan
                                            if (count_isbn_root_match_rows > 0)
                                            {

                                                apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                goto ENDLOOP;

                                            }

                                        }

                                    }

                                }

                                reader_eka.Close();
                                conn_eka.Close();

                            }


                            // Jos samalla ISBN-tunnuksella ei loydy muita julkaisuja ODS_Julkaisut-taulusta, niin mennaan tanne
                            else if (count_ISBN_match_rows <= 0)
                            {

                                // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                if (!(juuri.Equals("-1")))
                                {

                                    // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                    int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                    // Jos julkaisukanavatietokannasta loytyy yksi tai useampi rivi, jolle ISBN-juuri matchaa, niin haetaan nama julkaisukanavat 
                                    // ja tarkistetaan tasmaavatko myos julkaisussa oleva kustantajan nimi julkaisukanavatietokannassa oleviin Name- tai Other_Title -kenttiin
                                    if (count_isbn_root_match_rows > 0)
                                    {

                                        apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                        goto ENDLOOP;

                                    }

                                }

                            }

                        }

                    }


                    // Tutkitaan lopuksi julkaisutyypit A4 ja C2
                    else if (ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C2"))
                    {

                        // Jos count_konferenssi_match_rows > 0, niin asetetaan JufoTunnus ja JufoLuokkaKoodi vastaavan vuoden mukaan samaan tapaan
                        // kuin A1- ja A2 -julkaisutyyppien kanssa tehtiin.
                        if (count_konferenssi_match_rows > 0)
                        {

                            apufunktiot.jufo_tarkistus_konferenssin_nimella(server, ekaKonferenssi, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi, "exact", "");
                            goto ENDLOOP;

                        }  


                        // Jos mennaan tahan silmukkaan, niin talloin julkaisukanavatietokannasta ei loydy sellaista kanavaa, 
                        // jonka Name tai Other_Title matchaisi SA_Julkaisut-taulun julkaisun konferenssin nimen kanssa.
                        else if (count_konferenssi_match_rows <= 0)
                        {

                            // tarkistetaan onko julkaisulle ilmoitettu ISSN-tunnus
                            // Jos mennaan tahan haaraan, niin ISSN-tunnus on annettu
                            if (ISSN_tunnusten_maara_SA > 0)
                            {

                                // Jos count_ISSN_match_rows > 0, niin loytyy ISSN-matcheja
                                if (count_ISSN_match_rows > 0)
                                {

                                    apufunktiot.jufo_tarkistus_issn_tunnuksella(server, ekaISSN1, ekaISSN2, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                    goto ENDLOOP;

                                }

                                // jos ISSN-matcheja ei loydy, niin mennaan tahan silmukkaan
                                else
                                {

                                    // jos julkaisun ISBN-tunnuksella loytyy muita julkaisuja ODS_Julkaisut -taulusta, niin mennaan tanne
                                    if (count_ISBN_match_rows > 0)
                                    {

                                        SqlConnection conn_eka = new SqlConnection(connectionString_ods_julkaisut);
                                        SqlDataReader reader_eka = tietokantaoperaatiot.ODS_ISBN_hae_julkaisunTunnus(conn_eka, server, ekaISBN1, ekaISBN2);

                                        while (reader_eka.Read())
                                        {

                                            string ODS_julkTunnus = reader_eka["JulkaisunTunnus"] == System.DBNull.Value ? null : (string)reader_eka["JulkaisunTunnus"];

                                            // tutkitaan loytyyko kyseiselle julkaisunTunnukselle ISSN-tunnusta ODS-alueelta
                                            bool ODS_julkaisulle_loytyy_issn_tunnus = tietokantaoperaatiot.ODS_julkaisulle_loytyy_issn_tunnus(server, ODS_julkTunnus);

                                            if (ODS_julkaisulle_loytyy_issn_tunnus) // julkaisulle loytyy ISSN-tunnus
                                            {

                                                // Haetaan kyseinen ISSN-tunnus
                                                string ODS_ISSN_tunnus = tietokantaoperaatiot.ODS_ISSN_hae_issn_tunnus(server, ODS_julkTunnus);

                                                // Tutkitaan sitten loytyyko julkaisukanavatietokannasta riveja kyseisella ISSN-tunnuksella
                                                int julkaisukanavatietokanta_count_rows_based_on_ODS_ISSN = tietokantaoperaatiot.Julkaisukanavatietokanta_count_issn_match_rows(server, ODS_ISSN_tunnus, "");

                                                // Jos julkaisukanavatietokannasta loytyy ODS_Julkaisut -taulun ISSN-tunnusta vastaava match, niin mennaan tahan haaraan
                                                if (julkaisukanavatietokanta_count_rows_based_on_ODS_ISSN > 0)
                                                {

                                                    apufunktiot.jufo_tarkistus_issn_tunnuksella(server, ODS_ISSN_tunnus, "", ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                    goto ENDLOOP;

                                                }

                                                // Jos julkaisukanavatietokannasta ei loydy ODS_Julkaisut -taulun ISSN-tunnusta vastaavaa matchia, niin mennaan tahan haaraan
                                                else
                                                {

                                                    // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                                    // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                                    // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                                    string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                                    // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                                    if (!(juuri.Equals("-1")))
                                                    {

                                                        // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                                        int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                                        // Jos loytyy yksi tai useampi rivi, joilla on ISBN-juuri, joka vastaa SA_Julkaisut-taulun ISBN-juurta, niin mennaan tahan silmukkaan
                                                        if (count_isbn_root_match_rows > 0)
                                                        {

                                                            apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                            goto ENDLOOP;

                                                        }

                                                    }

                                                }

                                            }


                                            else if (ODS_julkaisulle_loytyy_issn_tunnus == false)   // ei loydy ISSN-tunnusta
                                            {

                                                // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                                // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                                // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                                string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                                // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                                if (!(juuri.Equals("-1")))
                                                {

                                                    // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                                    int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                                    // Jos loytyy yksi tai useampi rivi, joilla on ISBN-juuri, joka vastaa SA_Julkaisut-taulun ISBN-juurta, niin mennaan tahan silmukkaan
                                                    if (count_isbn_root_match_rows > 0)
                                                    {

                                                        apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                        goto ENDLOOP;

                                                    }

                                                }

                                            }

                                        }

                                        reader_eka.Close();
                                        conn_eka.Close();

                                    }


                                    // Jos samalla ISBN-tunnuksella ei loydy muita julkaisuja ODS_Julkaisut-taulusta, niin mennaan tanne
                                    else if (count_ISBN_match_rows <= 0)
                                    {

                                        // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                        // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                        // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                        string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                        // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                        if (!(juuri.Equals("-1")))
                                        {

                                            // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                            int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                            // Jos julkaisukanavatietokannasta loytyy yksi tai useampi rivi, jolle ISBN-juuri matchaa, niin haetaan nama julkaisukanavat 
                                            // ja tarkistetaan tasmaavatko myos julkaisussa oleva kustantajan nimi julkaisukanavatietokannassa oleviin Name- tai Other_Title -kenttiin
                                            if (count_isbn_root_match_rows > 0)
                                            {

                                                apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                                goto ENDLOOP;

                                            }

                                        }

                                    }

                                }

                            }


                            // Jos mennaan tahan haaraan, niin julkaisulle ei ole annettu ISSN-tunnusta
                            else if (ISSN_tunnusten_maara_SA <= 0)
                            {

                                // Tarkistetaan etta julkaisulle on annettu ISBN-tunnus
                                if (ISBN_tunnusten_maara_SA > 0)
                                {

                                    // Tutkitaan sitten mika on ekaISBN:aa vastaava ISBN-juuri
                                    // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
                                    // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
                                    string juuri = apufunktiot.parsi_ISBN_juuri(ekaISBN1);

                                    // Jos ISBN-juuri pystytaan parsimaan, niin mennaan tahan silmukkaan
                                    if (!(juuri.Equals("-1")))
                                    {

                                        // Tutkitaan onko julkaisukanavatietokannassa riveja, joilta loytyy ISBN-juuri
                                        int count_isbn_root_match_rows = tietokantaoperaatiot.Julkaisukanavatietokanta_count_isbn_root_match_rows(server, juuri);

                                        // Jos loytyy yksi tai useampi rivi, joilla on ISBN-juuri, joka vastaa SA_Julkaisut-taulun ISBN-juurta, niin mennaan tahan silmukkaan
                                        if (count_isbn_root_match_rows > 0)
                                        {

                                            apufunktiot.jufo_tarkistus_isbn_juurella(server, juuri, ekaKustantaja, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);
                                            goto ENDLOOP;

                                        }

                                    }

                                }

                            }

                        }

                    }


                    else if (ekaJulkaisutyyppi.Equals("B1") || ekaJulkaisutyyppi.Equals("B2") || ekaJulkaisutyyppi.Equals("B3") || ekaJulkaisutyyppi.Equals("D1") || ekaJulkaisutyyppi.Equals("D2") || ekaJulkaisutyyppi.Equals("D3") ||
                        ekaJulkaisutyyppi.Equals("D4") || ekaJulkaisutyyppi.Equals("D5") || ekaJulkaisutyyppi.Equals("D6") || ekaJulkaisutyyppi.Equals("E1") || ekaJulkaisutyyppi.Equals("E2") || ekaJulkaisutyyppi.Equals("E3"))
                    {

                        // Jos count_ISSN_match_rows > 0, niin loytyy ISSN-matcheja
                        if (count_ISSN_match_rows > 0)
                        {

                            apufunktiot.jufo_tarkistus_issn_tunnuksella(server, ekaISSN1, ekaISSN2, ekaJulkaisuvuosi, ekaJulkaisunTunnus, ekaJulkaisutyyppi);

                        }

                    }

                ENDLOOP: ;

                }

                //Console.Write("lopussa ollaan");
                reader_SA.Close();
                conn_SA.Close();

            }

            //Console.ReadLine();

            Environment.Exit(0);

        }

    }

}