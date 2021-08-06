using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Jufo_Tunnistus
{
    class Apufunktiot 
    {

        // nama stop wordsit hypataan yli eli naita ei oteta huomioon pitkissa stringeissa (julkaisun nimi, kustantaja jne.)
        private string[] stop_words = {" i "," me "," my "," myself "," we "," our "," ours "," ourselves "," you "," your "," yours "," yourself "," yourselves "," he "," him "," his "," himself "," she "," her "," hers "," herself "," it "," its "," itself "," they "," them "," their "," theirs ",
" themselves "," what "," which "," who "," whom "," this "," that "," these "," those "," am "," is "," are "," was "," were "," be "," been "," being "," have "," has "," had "," having "," do "," does "," did "," doing "," a "," an "," the "," and "," but "," if "," or "," because "," as "," until ",
" while "," of "," at "," by "," for "," with "," about "," against "," between "," into "," through "," during "," before "," after "," above "," below "," to "," from "," up "," down "," in "," out "," on "," off "," over "," under "," again "," further "," then "," once "," here "," there ",
" when "," where "," why "," how "," all "," any "," both "," each "," few "," more "," most "," other "," some "," such "," no "," nor "," not "," only "," own "," same "," so "," than "," too "," very "," s "," t "," can "," will "," just "," don "," should "," now "};

        // nama stop charsit hypataan yli eli naita ei oteta mukaan pitkissa stringeissa (julkaisun nimi, kustantaja jne.)
        private string[] stop_chars = { "!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "£", "¿", 
                                        "®", "¬", "½", "¼", "«", "»", "©", "┐", "└", "┴", "┬", "├", "─", "┼", "┘", "┌", "¦", "¯", "´", "≡", "±", "‗", "¾", "¶", "§", "÷", "¸", "°", "¨", "·", "¹", "³", "²" };

        Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot();

        // Muokataan parametrina annettua nimea siten, etta nimesta poistetaan stop wordsit ja stop charsit.
        // Lisaksi alusta poistetaan the, a ja an -merkit ja merkkijono trimmataan.
        // Palautetaan muokattu merkkijono.
        public string muokkaa_nimea(string nimi)
        {
            // Muutetaan nimi LowerCase:ksi ja trimmataan
            nimi = nimi.ToLower().Trim();

            // Kaydaan lapi stop_chars -merkit ja poistetaan merkki mikali se loytyy nimesta
            foreach (string c in stop_chars)
            {
                if (nimi.Contains(c))
                {
                    nimi = nimi.Replace(c, " ");
                }
            }

            // Trimmataan taas nimi
            nimi = nimi.Trim();

            // Kaydaan lapi stop_words -sanat ja poistetaan sana mikali se loytyy nimesta
            foreach (string item in stop_words)
            {
                if (nimi.Contains(item))
                {
                    nimi = nimi.Replace(item, " ");
                }
            }

            // poistetaan tyhjat valimerkit
            nimi = nimi.Replace("     ", " ");
            nimi = nimi.Replace("    ", " ");
            nimi = nimi.Replace("   ", " ");
            nimi = nimi.Replace("  ", " ");

            // Jalleen trimmataan nimi
            nimi = nimi.Trim();

            // Poistetaan sitten nimen alusta sanat the, a ja an
            string sana = "";

            for (int i = 0; i < nimi.Length; i++)
            {

                if (nimi[i] != ' ')
                {
                    sana = sana + nimi[i];
                }

                else
                {
                    sana = sana + nimi[i];

                    if (sana.Equals("the ") || sana.Equals("a ") || sana.Equals("an "))
                    {
                        nimi = nimi.Replace(sana, "").Trim();
                    }

                    break;
                }


            }

            return nimi;

        }


        // Haetaan julkaisulle jufo-luokka parametrina annetun julkaisuvuoden mukaan.
        // Parametrina annetaan myos jufoHist, joka kertoo kunkin vuoden jufo-luokat
        // julkaisukanavalle
        public string hae_jufoLuokka_julkaisuvuoden_mukaan(int julkVuosi, string jufoHist)
        {

            string year_target_in_string = "";    // alustetaan tyhjaksi merkkijonoksi
            int year_target = 0;                  // alustetaan nollaksi

            for (int i = 0; i < (jufoHist.Length - 5); i++)
            {

                /* kaydaan lapi jufo_history-kentta ja poimitan sielta vuosi
                   ja sita vastaava jufo-luokka */

                // tassa "luodaan" vuosiluku
                for (int j = i; j < (i + 4); j++)
                {
                    if (jufoHist[j] == ';' || jufoHist[j] == ':')
                    {
                        year_target_in_string = "";
                        break;
                    }
                    else
                    {
                        year_target_in_string = year_target_in_string + jufoHist[j];

                        // konvertoidaan vuosiluku string -> int
                        year_target = int.Parse(year_target_in_string);
                    }
                }


                if (year_target_in_string.Length == 4)
                {

                    // tutkitaan matchaavatko year_target ja ekaJulkaisuvuosi
                    if (julkVuosi == year_target)    // match
                    {
                        // tarkistetaan, onko julkaisukanavalla jufo-luokka ko. vuodelle
                        string jufo_level = "" + jufoHist[i + 5];

                        if (!jufo_level.Equals(";"))    // jufo_level ei ole tyhja
                        {

                            return jufo_level;  // palautetaan jufo-luokka

                        }
                    }
                }

                year_target_in_string = "";
            }


            // jos tunnusta ei loydy, niin palautetaan -1
            return "-1";


        }


        // Tassa funktiossa parsitaan parametrina annettu ISBN-tunnus ja palautetaan sen ISBN-juuri
        // ISBN-juuri on ISBN-tunnuksen kolme ensimmaista valiviivoin erotettua lukusarjaa
        // esim jos ISBN-tunnus on 978-951-98548-9-2, niin ISBN-juuri on 978-951-98548
        public string parsi_ISBN_juuri(string isbn)
        {

            string juuri = "";  // alustetaan tyhjaksi merkkijonoksi
            int valiviivojenMaara = 0;  // tama muuttuja kertoo kuinka monta valiviivaa on kayty lapi

            for (int i = 0; i < isbn.Length; i++)
            {

                string merkki = "" + isbn[i];

                // Tutkitaan tapaus, kun merkki on valiviiva
                if (merkki.Equals("-"))
                {

                    valiviivojenMaara = valiviivojenMaara + 1;

                    if (valiviivojenMaara == 3)
                    {

                        return juuri;

                    }

                    else
                    {

                        juuri = juuri + isbn[i];

                    }

                }


                // Tutkitaan sitten tapaus, jossa merkki on numero
                else
                {

                    juuri = juuri + isbn[i];

                }

            }


            // Jos mennaan tanne, niin jokin meni vikaan
            return "-1";

        }


        // Tassa funktiossa haetaan julkaisukanavan tiedot ISSN-tunnuksilla ja sitten paivitetaan jufoTunnus ja jufoLuokkaKoodi kantaan
        public void jufo_tarkistus_issn_tunnuksella(string server, string ekaISSN1, string ekaISSN2, int ekaJulkaisuvuosi, string ekaJulkaisunTunnus, string ekaJulkaisutyyppi) 
        {

            string jufo_level = ""; // tama on muuttuja, johon haetaan jufo-luokka julkaisukanavatietokannasta

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            // Haetaan Julkaisukanavatietokanta-taulusta kaikki tiedot julkaisulle ISSN-tunnuksen perusteella
            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);
            SqlDataReader reader = tietokantaoperaatiot.Julkaisukanavatietokanta_select_ISSN_tunnuksella(conn, ekaISSN1, ekaISSN2);

            // kaydaan lapi hakutulokset ja otetaan ne talteen muuttujiin.
            while (reader.Read())
            {

                string jufo_ID = reader["Jufo_ID"] == System.DBNull.Value ? null : (string) reader["Jufo_ID"];
                string channel_ID = reader["Channel_ID"] == System.DBNull.Value ? null : (string) reader["Channel_ID"];
                string active = reader["Active"] == System.DBNull.Value ? null : (string) reader["Active"];
                string jufo_history = reader["Jufo_History"] == System.DBNull.Value ? null : (string) reader["Jufo_History"];
                int year_end = (int) reader["Year_End"];
                int active_binary = (int) reader["Active_binary"];

                if (active_binary == 1)
                {

                    if ((jufo_ID != null) && !(jufo_ID.Equals("")))
                    {
                        
                        // Sitten tutkitaan, onko kyseessa aktiivinen vai lopettanut kanava.
                        if (active.Equals("Active"))    // kanava on aktiivinen
                        {

                            /* Jos kanava on aktiivinen, niin tutkitaan mika on julkaisun julkaisuvuosi.
                                Sitten tata julkaisuvuotta vastaava Jufo_luokka poimitaan Julkaisukanavatietokanta-
                                taulun jufo_history-sarakkeesta */

                            // Haetaan julkaisuvuotta vastaava jufo-luokka
                            jufo_level = hae_jufoLuokka_julkaisuvuoden_mukaan(ekaJulkaisuvuosi, jufo_history);

                            if (!(jufo_level.Equals("-1")))   // Loydetaan jufoLuokka, joka vastaa julkaisuvuotta
                            {

                                // kanavan tyypin pitaa olla A1, A2, A3, A4, C1 tai C2, jotta sille annetaan jufoluokka
                                if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2") || ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C1") || ekaJulkaisutyyppi.Equals("C2"))
                                {
                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, jufo_level);
                                }

                                // kanavan tyyppi on eri kuin A1, A2, A3, A4, C1 tai C2
                                else
                                {
                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                }
                                    
                            }
                            else    // ei loydeta jufoLuokkaa, joten asetetaan JufoLuokka nulliksi
                            {
                                tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                            }

                        }


                        // Jos kanava ei ole aktiivinen, niin Active = "Inactive"
                        else if (active.Equals("Inactive"))     // kanava ei ole aktiivinen
                        {

                            if (ekaJulkaisuvuosi <= year_end)
                            {

                                jufo_level = hae_jufoLuokka_julkaisuvuoden_mukaan(ekaJulkaisuvuosi, jufo_history);

                                if (!(jufo_level.Equals("-1")))   // Loydetaan jufoLuokka, joka vastaa julkaisuvuotta
                                {

                                    // kanavan tyypin pitaa olla A1, A2, A3, A4, C1 tai C2, jotta sille annetaan jufoluokka
                                    if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2") || ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C1") || ekaJulkaisutyyppi.Equals("C2"))
                                    {
                                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, jufo_level);
                                    }

                                    // kanavan tyyppi on eri kuin A1, A2, A3, A4, C1 tai C2
                                    else
                                    {
                                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                    }

                                }

                                else     // ei loydeta jufoLuokkaa, joten asetetaan JufoLuokka nulliksi
                                {
                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                }

                            }

                        }

                    }

                    else if (((jufo_ID == null) || (jufo_ID.Equals(""))) && (channel_ID != null) && !(channel_ID.Equals("")))
                    {

                        if (!(ekaJulkaisutyyppi.Equals("A1")) && !(ekaJulkaisutyyppi.Equals("A2")) && !(ekaJulkaisutyyppi.Equals("A3")) && !(ekaJulkaisutyyppi.Equals("A4")) && !(ekaJulkaisutyyppi.Equals("C1")) && !(ekaJulkaisutyyppi.Equals("C2")))
                        {
                           
                            // Sitten tutkitaan, onko kyseessa aktiivinen vai lopettanut kanava.
                            if (active.Equals("Active"))    // kanava on aktiivinen
                            {
                                tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, channel_ID, null);
                            }

                            // Jos kanava ei ole aktiivinen, niin Active = "Inactive"
                            else if (active.Equals("Inactive"))     // kanava ei ole aktiivinen
                            {

                                if (ekaJulkaisuvuosi <= year_end)
                                {
                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, channel_ID, null);
                                }

                            }

                        }

                    }
                    
                }

            }

            reader.Close();
            conn.Close();

        }


        // Tassa funktiossa haetaan julkaisukanavan tiedot ISBN-juurella ja sitten paivitetaan jufoTunnus ja jufoLuokkaKoodi kantaan
        public void jufo_tarkistus_isbn_juurella(string server, string juuri, string ekaKustantaja, int ekaJulkaisuvuosi, string ekaJulkaisunTunnus, string ekaJulkaisutyyppi)
        {

            string jufo_level = ""; // tama on muuttuja, johon haetaan jufo-luokka julkaisukanavatietokannasta

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            // Haetaan Julkaisukanavatietokanta-taulusta kaikki tiedot julkaisulle ISBN_Root-tunnuksen perusteella
            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);
            SqlDataReader reader = tietokantaoperaatiot.Julkaisukanavatietokanta_select_ISBN_Root_perusteella(conn, juuri);

            // kaydaan lapi hakutulokset ja otetaan ne talteen muuttujiin.
            while (reader.Read())
            {

                string jufo_ID = reader["Jufo_ID"] == System.DBNull.Value ? null : (string)reader["Jufo_ID"];
                string channel_ID = reader["Channel_ID"] == System.DBNull.Value ? null : (string)reader["Channel_ID"];
                string name = reader["Name"] == System.DBNull.Value ? null : (string) reader["Name"];
                string other_title = reader["Other_Title"] == System.DBNull.Value ? null : (string) reader["Other_Title"];
                // Muutetaan name ja other_title " & "-merkki " and "-merkiksi
                if (name != null)
                {
                    name = name.Replace(" & ", " and ");
                }

                if (other_title != null)
                {
                    other_title = other_title.Replace(" & ", " and ");
                }

                string type = reader["Type"] == System.DBNull.Value ? null : (string) reader["Type"];
                string active = reader["Active"] == System.DBNull.Value ? null : (string) reader["Active"];
                string jufo_history = reader["Jufo_History"] == System.DBNull.Value ? null : (string) reader["Jufo_History"];
                int year_end = (int) reader["Year_End"];
                int active_binary = (int) reader["Active_binary"];

                if (active_binary == 1)
                {

                    // Verrataan onko KustantajanNimi = Name tai KustantajanNimi = Other_Title
                    // Ensin tarkistetaan kuitenkin, etta ekaKustantaja != "" ja ekaKustantaja != null
                    if ((ekaKustantaja != null) && !(ekaKustantaja.Equals("")))
                    {

                        if (ekaKustantaja.ToLower().Equals(name.ToLower()))    // kustantaja ja nimi on muutettu lowercase:ksi
                        {

                            // Verrataan onko type = Kirjakustantaja
                            if (type.Equals("Kirjakustantaja"))
                            {

                                if ((jufo_ID != null) && !(jufo_ID.Equals("")))
                                {


                                    // Sitten tutkitaan, onko kyseessa aktiivinen vai lopettanut kanava.
                                    if (active.Equals("Active"))    // kanava on aktiivinen
                                    {

                                        /* Jos kanava on aktiivinen, niin tutkitaan mika on julkaisun julkaisuvuosi.
                                            Sitten tata julkaisuvuotta vastaava Jufo_luokka poimitaan jfp_Testitaulu-
                                            taulun jufo_history-sarakkeesta */

                                        // Haetaan julkaisuvuotta vastaava jufo-luokka
                                        jufo_level = hae_jufoLuokka_julkaisuvuoden_mukaan(ekaJulkaisuvuosi, jufo_history);

                                        if (!(jufo_level.Equals("-1")))
                                        {

                                            // kanavan tyypin pitaa olla A1, A2, A3, A4, C1 tai C2, jotta sille annetaan jufoluokka
                                            if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2") || ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C1") || ekaJulkaisutyyppi.Equals("C2"))
                                            {
                                                tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, jufo_level);
                                            }

                                            // kanavan tyyppi on eri kuin A1, A2, A3, A4, C1 tai C2
                                            else
                                            {
                                                tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                            }

                                        }
                                        else     // ei loydeta jufoLuokkaa, joten asetetaan JufoLuokka nulliksi
                                        {
                                            tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                        }

                                        break;

                                    }


                                    // Jos kanava ei ole aktiivinen, niin Active = "Inactive"
                                    else if (active.Equals("Inactive"))     // kanava ei ole aktiivinen
                                    {

                                        if (ekaJulkaisuvuosi <= year_end)
                                        {

                                            jufo_level = hae_jufoLuokka_julkaisuvuoden_mukaan(ekaJulkaisuvuosi, jufo_history);

                                            if (!(jufo_level.Equals("-1")))
                                            {

                                                // kanavan tyypin pitaa olla A1, A2, A3, A4, C1 tai C2, jotta sille annetaan jufoluokka
                                                if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2") || ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C1") || ekaJulkaisutyyppi.Equals("C2"))
                                                {
                                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, jufo_level);
                                                }

                                                // kanavan tyyppi on eri kuin A1, A2, A3, A4, C1 tai C2
                                                else
                                                {
                                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                                }

                                            }
                                            else     // ei loydeta jufoLuokkaa, joten asetetaan JufoLuokka nulliksi
                                            {
                                                tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                            }

                                        }

                                        break;

                                    }

                                }

                                else if (((jufo_ID == null) || (jufo_ID.Equals(""))) && (channel_ID != null) && !(channel_ID.Equals("")))
                                {

                                    if (!(ekaJulkaisutyyppi.Equals("A1")) && !(ekaJulkaisutyyppi.Equals("A2")) && !(ekaJulkaisutyyppi.Equals("A3")) && !(ekaJulkaisutyyppi.Equals("A4")) && !(ekaJulkaisutyyppi.Equals("C1")) && !(ekaJulkaisutyyppi.Equals("C2")))
                                    {

                                        // Sitten tutkitaan, onko kyseessa aktiivinen vai lopettanut kanava.
                                        if (active.Equals("Active"))    // kanava on aktiivinen
                                        {
                                            tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, channel_ID, null);
                                        }

                                        // Jos kanava ei ole aktiivinen, niin Active = "Inactive"
                                        else if (active.Equals("Inactive"))     // kanava ei ole aktiivinen
                                        {

                                            if (ekaJulkaisuvuosi <= year_end)
                                            {
                                                tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, channel_ID, null);
                                            }

                                        }

                                    }

                                }

                            }

                        }

                        else if ((other_title != null) && !(other_title.Equals("")))
                        {

                            // Splitataan other_title siten, etta erotinmerkkina on puolipiste 
                            // other_title -kentassa voi olla esimerkiksi arvo Association for computing machinery;ACM Press, joka pitaisi erotella kahteen osaan puolipisteen kohdalta.
                            string[] other_titles = other_title.Split(';');
                            
                            foreach (string o in other_titles)
                            {

                                string o_trimmed = o.Trim();  // poistetaa tyhjat merkit alusta ja lopusta

                                if (ekaKustantaja.ToLower().Equals(o_trimmed.ToLower()))
                                {

                                    // Verrataan onko type = Kirjakustantaja
                                    if (type.Equals("Kirjakustantaja"))
                                    {

                                        if ((jufo_ID != null) && !(jufo_ID.Equals("")))
                                        {

                                            // Lisataan SA_Julkaisut-taulun julkaisulle JufoTunnus ja Jufo_Level
                                            // Sitten tutkitaan, onko kyseessa aktiivinen vai lopettanut kanava.
                                            if (active.Equals("Active"))    // kanava on aktiivinen
                                            {

                                                /* Jos kanava on aktiivinen, niin tutkitaan mika on julkaisun julkaisuvuosi.
                                                    Sitten tata julkaisuvuotta vastaava Jufo_luokka poimitaan jfp_Testitaulu-
                                                    taulun jufo_history-sarakkeesta */

                                                // Haetaan julkaisuvuotta vastaava jufo-luokka
                                                jufo_level = hae_jufoLuokka_julkaisuvuoden_mukaan(ekaJulkaisuvuosi, jufo_history);

                                                if (!(jufo_level.Equals("-1")))
                                                {

                                                    // kanavan tyypin pitaa olla A1, A2, A3, A4, C1 tai C2, jotta sille annetaan jufoluokka
                                                    if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2") || ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C1") || ekaJulkaisutyyppi.Equals("C2"))
                                                    {
                                                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, jufo_level);
                                                    }

                                                    // kanavan tyyppi on eri kuin A1, A2, A3, A4, C1 tai C2
                                                    else
                                                    {
                                                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                                    }

                                                }
                                                else     // ei loydeta jufoLuokkaa, joten asetetaan JufoLuokka nulliksi
                                                {
                                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                                }

                                                break;

                                            }


                                            // Jos kanava ei ole aktiivinen, niin Active = "Inactive"
                                            else if (active.Equals("Inactive"))     // kanava ei ole aktiivinen
                                            {

                                                if (ekaJulkaisuvuosi <= year_end)
                                                {

                                                    jufo_level = hae_jufoLuokka_julkaisuvuoden_mukaan(ekaJulkaisuvuosi, jufo_history);

                                                    if (!(jufo_level.Equals("-1")))
                                                    {

                                                        // kanavan tyypin pitaa olla A1, A2, A3, A4, C1 tai C2, jotta sille annetaan jufoluokka
                                                        if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2") || ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C1") || ekaJulkaisutyyppi.Equals("C2"))
                                                        {
                                                            tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, jufo_level);
                                                        }

                                                        // kanavan tyyppi on eri kuin A1, A2, A3, A4, C1 tai C2
                                                        else
                                                        {
                                                            tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                                        }

                                                    }
                                                    else     // ei loydeta jufoLuokkaa, joten asetetaan JufoLuokka nulliksi
                                                    {
                                                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                                    }

                                                }

                                                break;

                                            }

                                        }

                                        else if (((jufo_ID == null) || (jufo_ID.Equals(""))) && (channel_ID != null) && !(channel_ID.Equals("")))
                                        {

                                            if (!(ekaJulkaisutyyppi.Equals("A1")) && !(ekaJulkaisutyyppi.Equals("A2")) && !(ekaJulkaisutyyppi.Equals("A3")) && !(ekaJulkaisutyyppi.Equals("A4")) && !(ekaJulkaisutyyppi.Equals("C1")) && !(ekaJulkaisutyyppi.Equals("C2")))
                                            {

                                                // Sitten tutkitaan, onko kyseessa aktiivinen vai lopettanut kanava.
                                                if (active.Equals("Active"))    // kanava on aktiivinen
                                                {
                                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, channel_ID, null);
                                                }

                                                // Jos kanava ei ole aktiivinen, niin Active = "Inactive"
                                                else if (active.Equals("Inactive"))     // kanava ei ole aktiivinen
                                                {

                                                    if (ekaJulkaisuvuosi <= year_end)
                                                    {
                                                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, channel_ID, null);
                                                    }

                                                }

                                            }

                                        }

                                    }

                                }

                            }

                        }

                    }

                }

            }

            reader.Close();
            conn.Close();

        }


        public void jufo_tarkistus_konferenssin_nimella(string server, string ekaKonferenssi, int ekaJulkaisuvuosi, string ekaJulkaisunTunnus, string ekaJulkaisutyyppi, string matchType, string jufo)
        {

            string jufo_level = ""; // tama on muuttuja, johon haetaan jufo-luokka julkaisukanavatietokannasta

            string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";

            // Haetaan Julkaisukanavatietokanta-taulusta tiedot julkaisulle Name- tai Other_Title -kenttien perusteella
            SqlConnection conn = new SqlConnection(connectionString_mds_julkaisut);
            SqlDataReader reader = null;

            if (matchType.Equals("exact"))
            {
                reader = tietokantaoperaatiot.Julkaisukanavatietokanta_select_konferenssin_nimella(conn, ekaKonferenssi);
            }

            else if (matchType.Equals("nonExact"))
            {
                reader = tietokantaoperaatiot.Julkaisukanavatietokanta_select_jufolla(conn, jufo);
            }

            // kaydaan lapi hakutulokset ja otetaan ne talteen muuttujiin.
            while (reader.Read())
            {

                string jufo_ID = reader["Jufo_ID"] == System.DBNull.Value ? null : (string)reader["Jufo_ID"];
                string channel_ID = reader["Channel_ID"] == System.DBNull.Value ? null : (string)reader["Channel_ID"];
                string type = reader["Type"] == System.DBNull.Value ? null : (string) reader["Type"];
                string active = reader["Active"] == System.DBNull.Value ? null : (string) reader["Active"];
                string jufo_history = reader["Jufo_History"] == System.DBNull.Value ? null : (string) reader["Jufo_History"];
                int year_end = (int) reader["Year_End"];
                int active_binary = (int) reader["Active_binary"];


                if (active_binary == 1)
                {

                    if ((jufo_ID != null) && !(jufo_ID.Equals("")))
                    {

                        // Sitten tutkitaan, onko kyseessa aktiivinen vai lopettanut kanava.
                        if (active.Equals("Active"))    // kanava on aktiivinen
                        {

                            /* Jos kanava on aktiivinen, niin tutkitaan mika on julkaisun julkaisuvuosi.
                                Sitten tata julkaisuvuotta vastaava Jufo_luokka poimitaan Julkaisukanavatietokanta-
                                taulun jufo_history-sarakkeesta */

                            // Haetaan julkaisuvuotta vastaava jufo-luokka
                            jufo_level = hae_jufoLuokka_julkaisuvuoden_mukaan(ekaJulkaisuvuosi, jufo_history);

                            if (!(jufo_level.Equals("-1")))
                            {

                                // kanavan tyypin pitaa olla A1, A2, A3, A4, C1 tai C2, jotta sille annetaan jufoluokka
                                if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2") || ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C1") || ekaJulkaisutyyppi.Equals("C2"))
                                {
                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, jufo_level);
                                }

                                // kanavan tyyppi on eri kuin A1, A2, A3, A4, C1 tai C2
                                else
                                {
                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                }

                            }
                            else     // ei loydeta jufoLuokkaa, joten asetetaan JufoLuokka nulliksi
                            {
                                tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                            }

                            break;

                        }


                        // Jos kanava ei ole aktiivinen, niin Active = "Inactive"
                        else if (active.Equals("Inactive"))     // kanava ei ole aktiivinen
                        {

                            if (ekaJulkaisuvuosi <= year_end)
                            {

                                jufo_level = hae_jufoLuokka_julkaisuvuoden_mukaan(ekaJulkaisuvuosi, jufo_history);

                                if (!(jufo_level.Equals("-1")))
                                {

                                    // kanavan tyypin pitaa olla A1, A2, A3, A4, C1 tai C2, jotta sille annetaan jufoluokka
                                    if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2") || ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C1") || ekaJulkaisutyyppi.Equals("C2"))
                                    {
                                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, jufo_level);
                                    }

                                    // kanavan tyyppi on eri kuin A1, A2, A3, A4, C1 tai C2
                                    else
                                    {
                                        tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                    }

                                }
                                else     // ei loydeta jufoLuokkaa, joten asetetaan JufoLuokka nulliksi
                                {
                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, jufo_ID, null);
                                }

                            }

                            break;

                        }

                    }

                    else if (((jufo_ID == null) || (jufo_ID.Equals(""))) && (channel_ID != null) && !(channel_ID.Equals("")))
                    {

                        if (!(ekaJulkaisutyyppi.Equals("A1")) && !(ekaJulkaisutyyppi.Equals("A2")) && !(ekaJulkaisutyyppi.Equals("A3")) && !(ekaJulkaisutyyppi.Equals("A4")) && !(ekaJulkaisutyyppi.Equals("C1")) && !(ekaJulkaisutyyppi.Equals("C2")))
                        {

                            // Sitten tutkitaan, onko kyseessa aktiivinen vai lopettanut kanava.
                            if (active.Equals("Active"))    // kanava on aktiivinen
                            {
                                tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, channel_ID, null);
                            }

                            // Jos kanava ei ole aktiivinen, niin Active = "Inactive"
                            else if (active.Equals("Inactive"))     // kanava ei ole aktiivinen
                            {

                                if (ekaJulkaisuvuosi <= year_end)
                                {
                                    tietokantaoperaatiot.SA_Julkaisut_update_JufoTunnus_ja_JufoLuokkaKoodi(server, ekaJulkaisunTunnus, channel_ID, null);
                                }

                            }

                        }

                    }

                }

            }

            reader.Close();
            conn.Close();

        }

    }

}

