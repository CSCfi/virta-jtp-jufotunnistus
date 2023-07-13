using System;
using System.Data;

namespace Jufo_Tunnistus
{
    class Program
    {
        
        static void Main(string[] args)
        {


            if (args.Length != 1)
            {
                Console.Write("Argumenttien maara on vaara.");
                Environment.Exit(0);
            }

            // Palvelin
            string server = args[0];
            string connString = "Server=" + server + ";Trusted_Connection=true";

            // Täällä ovat tarvittavat apufunktiot ja tietokantaoperaatiot
            Apufunktiot apufunktiot = new Apufunktiot();
            Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot(connString);



            // ---------------------------------------------------------------------------------------------------------------
            //
            // Ohjelman rakenne
            //
            // 1. Alustus
            // - SA_julkaisut taulun lukeminen muistiin ja nimikenttien muokkaus
            // - Nimikenttien kirjoitus tauluun SA_julkaisutTMP
            // - ISSN ja ISBN kenttien päivitys tauluun SA_julkaisutTMP
            // - Indeksien päivitys
            //
            // 2. Tunnistus
            // - JufoID:n tunnistus julkaisukanavatietokannasta ja tietojen päivitys tauluun SA_julkaisutTMP 
            //
            // 3. Päivitys
            // - Alkuperäiset tiedot tauluun Jufot_TMP
            // - Tunnistetut tiedot tauluun SA_Julkaisut
            //
            // ---------------------------------------------------------------------------------------------------------------



            //// Vaihe 1


            string taulu = "julkaisut_ods.dbo.SA_JulkaisutTMP";

            tietokantaoperaatiot.tyhjenna_taulu(taulu);

            // Taulun SA_Julkaisut lukeminen ja muokkaus
            DataTable dt1 = tietokantaoperaatiot.lue_tietokantataulu_datatauluun();

            foreach (DataRow row in dt1.Rows)
            {
                row["KonferenssinNimi"] = apufunktiot.muokkaa_nimea(row["KonferenssinNimi"].ToString());               
                row["KustantajanNimi"] = apufunktiot.muokkaa_nimea(row["KustantajanNimi"].ToString());        
                
                // Jufo-tunnus ja -luokka asetetaan nulliksi   
                row["JufoTunnus"] = null;
                row["JufoLuokkaKoodi"] = null;

                // Jos halutaan tehdä samalla duplikaatti-/yhteisjulkaisutarkistuksen edellyttämät muokkaukset niin alla olevat mukaan. 
                // Vastaavat sarakkeet mukaan myös funktiossa lue_tietokantataulu_datatauluun.
                // Lisäksi huomioitava eroavuudet perusjoukon valinnassa em. funktiossa, esim. siirrettävä julkaisutyyppirajaus funktioon tunnista_ISSN.
                //row["JulkaisunNimi"] = apufunktiot.muokkaa_nimea(row["JulkaisunNimi"].ToString());
                //row["EmojulkaisunNimi"] = apufunktiot.muokkaa_nimea(row["EmojulkaisunNimi"].ToString());
                //row["Lehdennimi"] = apufunktiot.muokkaa_nimea(row["Lehdennimi"].ToString());
                //row["DOI"] = apufunktiot.muokkaa_DOI(row["DOI"].ToString());
            }

            tietokantaoperaatiot.kirjoita_datataulu_tietokantaan(dt1, taulu);
            tietokantaoperaatiot.paivita_ISSN_ja_ISBN_tunnukset(taulu);
            tietokantaoperaatiot.uudelleenrakenna_indeksit(taulu);

            //goto loppu;



            // ---------------------------------------------------------------------------------------------------------------


            //// Vaihe 2


            /*
             
                //Julkaisutyypit A1 ja A2
                if (ekaJulkaisutyyppi.Equals("A1") || ekaJulkaisutyyppi.Equals("A2"))
                {
                    1 ISSN match -> Tarkistus  
                }

                // Julkaisutyypit A3 ja C1
                if (ekaJulkaisutyyppi.Equals("A3") || ekaJulkaisutyyppi.Equals("C1"))
                {
                
                    1 Julkaisulla ISSN tunnus
		                - Löytyy julkaisukanavatietokannasta -> Tarkistus
	                2 Julkaisulla ei ISSN tunnusta tai ISSN ei löydy julkaisukanavatietokannasta mutta on ISBN
		                - ISBN match
			                - Jolla on ISSN
				                - Löytyy julkaisukanavatietokannasta -> Tarkistus
				                - Ei löydy julkaisukanavatietokannasta -> ISBN-juuri
			                - Jolla ei ISSN -> ISBN-juuri
		                - Ei ISBN matchia -> ISBN-juuri                                                       
                
                }

                // Julkaisutyypit A4 ja C2
                else if (ekaJulkaisutyyppi.Equals("A4") || ekaJulkaisutyyppi.Equals("C2"))
                {
                    Huom. Muutettu "Julkaisulla Ei ISSN tunnusta mutta on ISBN tunnus" käsittelyä siten että ei mennä suoraan ISBN-juuri tarkasteluun. 
                    Täten tarkastelu on kohdasta "Konferenssia ei löydy julkaisukanavatietokannasta" eteenpäin vastaava kuin A3 ja C1.

                    1 Konferenssi löytyy julkaisukanavatietokannasta -> Tarkistus
	                2 Konferenssia ei löydy julkaisukanavatietokannasta
		                - Julkaisulla ISSN tunnus joka löytyy julkaisukanavatietokannasta -> Tarkistus
			            - Julkaisulla Ei ISSN tunnusta tai sitä ei löydy julkaisukanavatietokannasta mutta on ISBN
				                - ISBN match
					                - Jolla on ISSN
						                - Löytyy julkaisukanavatietokannasta -> Tarkistus
						                - Ei löydy julkaisukanavatietokannasta -> ISBN-juuri
					                - Jolla ei ISSN -> ISBN-juuri
				                - Ei ISBN match -> ISBN-juuri                        
               
                }

                // Muut julkaisutyypit
                else if (ekaJulkaisutyyppi.Equals("B1") || ekaJulkaisutyyppi.Equals("B2") || ekaJulkaisutyyppi.Equals("B3") || ekaJulkaisutyyppi.Equals("D1") || ekaJulkaisutyyppi.Equals("D2") || ekaJulkaisutyyppi.Equals("D3") ||
                    ekaJulkaisutyyppi.Equals("D4") || ekaJulkaisutyyppi.Equals("D5") || ekaJulkaisutyyppi.Equals("D6") || ekaJulkaisutyyppi.Equals("E1") || ekaJulkaisutyyppi.Equals("E2") || ekaJulkaisutyyppi.Equals("E3"))
                {
                    1 ISSN match -> Tarkistus               
                }

            */


            tietokantaoperaatiot.uudelleenrakenna_indeksit("julkaisut_mds.dbo.Julkaisukanavatietokanta");

            tietokantaoperaatiot.tunnista_konferenssi();
            tietokantaoperaatiot.tunnista_ISSN();
            tietokantaoperaatiot.tunnista_ISBN();
            tietokantaoperaatiot.tunnista_ISBN_juuri();



            // ---------------------------------------------------------------------------------------------------------------


            //// Vaihe 3


            // Alkuperäiset tiedot Jufot_TMP-tauluun
            tietokantaoperaatiot.kirjoita_jufot_tmp_tauluun();
            // Tunnistetut tiedot SA_Julkaisut-tauluun
            tietokantaoperaatiot.paivita_jufot_sa_tauluun();



            // ---------------------------------------------------------------------------------------------------------------

            loppu:


            //Console.WriteLine("Loppu");
            //Console.ReadLine();
            ;


        }

    }

}

