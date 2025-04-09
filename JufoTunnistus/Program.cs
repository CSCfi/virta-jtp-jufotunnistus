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
            //string server ="dwitjutisql19";  //debuggaukseen
            string server = args[0];

            string connString = "Server=" + server + ";Trusted_Connection=true";


            // Täällä ovat tarvittavat apufunktiot ja tietokantaoperaatiot
            Apufunktiot apufunktiot = new Apufunktiot();
            Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot(connString);


            // ---------------------------------------------------------------------------------------------------------------
            //
            // Ohjelman rakenne
            //
            // 1 Alustus
            // - SA_julkaisut-taulun lukeminen ja nimikenttien muokkaus
            // - Nimikenttien kirjoitus julkaisut_temp-tauluun
            // - ISSN- ja ISBN-kenttien päivitys julkaisut_temp-tauluun
            // - Indeksien päivitys
            //
            // 2 Tunnistus
            // - JufoID:n tunnistus julkaisukanavatietokannasta
            // - Tunnistamatta jääneiden julkaisujen tietojen täydennys VirtaAdditions-taulusta
            // - Kanavien jufo-luokan päivitys ja rankkaus
            //
            // 3 Päivitys
            // - Alkuperäiset tiedot talteen
            // - Tunnistetut tiedot SA_Julkaisut-tauluun
            //
            // ---------------------------------------------------------------------------------------------------------------


            // Nämä taulut tulee olla luotuna tietokannassa. Olemassaolon tarkistuksen ja mahdollisen luonnin voisi tehdä myös tässä ohjelmassa.
            // Duplikaatti- ja yhteisjulkaisutunnistus käyttää samaa julkaisut_temp-taulua.

            string taulu_julkaisut_temp = "julkaisut_ods.dbo.SA_JulkaisutTMP"; // muokatut nimet
            string taulu_jufot_temp = "julkaisut_ods.dbo.SA_JulkaisutTMP_jufot"; // kaikki tunnistetut kanavat


            //// Vaihe 1


            string kysely = @"
                SELECT 
                    JulkaisunTunnus
                    ,JulkaisuVuosi
                    ,OrganisaatioTunnus
                    ,JulkaisunOrgTunnus
                    ,JulkaisutyyppiKoodi  
                    ,JufoTunnus
                    ,JufoLuokkaKoodi                 
                    ,KonferenssinNimi                                                         
                    ,KustantajanNimi   
                    --,JulkaisunNimi 
                    --,EmojulkaisunNimi
                    --,LehdenNimi   
                    --,DOI          
                FROM julkaisut_ods.dbo.SA_Julkaisut j
                WHERE JulkaisunTilaKoodi != -1
                AND JulkaisutyyppiKoodi in ('A1','A2','A3','A4','C1','C2','D1','D2','D3','D4','D5','D6','E1','E2','E3') 
                AND NOT EXISTS (
                    SELECT 1 
                    FROM julkaisut_ods.dbo.EiJufoTarkistusta ejt
	                WHERE ejt.julkaisuntunnus = j.JulkaisunTunnus
                )";

            tietokantaoperaatiot.Tyhjenna_taulu(taulu_julkaisut_temp);

            // Taulun SA_Julkaisut luku tietokannasta ja muokkaus
            DataTable dt1 = tietokantaoperaatiot.Lue_tietokantataulu_datatauluun(kysely);

            foreach (DataRow row in dt1.Rows)
            {
                row["KonferenssinNimi"] = apufunktiot.Muokkaa_nimea(row["KonferenssinNimi"].ToString());
                row["KustantajanNimi"] = apufunktiot.Muokkaa_nimea(row["KustantajanNimi"].ToString());

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

            tietokantaoperaatiot.Kirjoita_datataulu_tietokantaan(dt1, taulu_julkaisut_temp);
            tietokantaoperaatiot.Paivita_ISSN_ja_ISBN_tunnukset(taulu_julkaisut_temp);

            tietokantaoperaatiot.Uudelleenrakenna_indeksit(taulu_julkaisut_temp);



            // ---------------------------------------------------------------------------------------------------------------


            //// Vaihe 2


            /*
             
                Julkaisutyypit A1, A2, D1, D4, E1, E3:
                -----------------------------------------------
                1 ISSN match -> Tarkistus  
            

                Julkaisutyypit A3, C1, D2, D5, E2:
                -----------------------------------------------
                1 Julkaisulla ISSN tunnus
		            - Löytyy julkaisukanavatietokannasta -> Tarkistus
	            2 Julkaisulla ei ISSN tunnusta tai ISSN ei löydy julkaisukanavatietokannasta mutta on ISBN
		            - ISBN match
			            - Jolla on ISSN
				            - Löytyy julkaisukanavatietokannasta -> Tarkistus
				            - Ei löydy julkaisukanavatietokannasta -> ISBN-juuri
			            - Jolla ei ISSN -> ISBN-juuri
		            - Ei ISBN matchia -> ISBN-juuri                                                       
                
                
                Julkaisutyypit A4, C2, D3, D5:
                -----------------------------------------------
                Huom. Muutettu "Julkaisulla Ei ISSN tunnusta mutta on ISBN tunnus" käsittelyä siten että ei mennä suoraan ISBN-juuri tarkasteluun. 
                Täten tunnistus on kohdasta "Konferenssia ei löydy julkaisukanavatietokannasta" eteenpäin vastaava kuin julkaisutyypeillä A3 ja C1.

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
                             

                Muut julkaisutyypit (B1, B2, B3):
                ---------------------------------------------------------------------
                Ei mukana tunnistuksessa. Jos halutaan mukaan, niin muokattava ensimmäistä kyselyä ja Tunnista-metodeja.

            */


            /* 
                Tunnistukset ajetaan yksi kerrallaan:
                    - Tunnistus tehdään sellaisille julkaisuille, joita ei ole jufot_temp-taulussa
                    - Tunnistetut kanavat kirjoitetaan jufot_temp-tauluun
                    - Tarkistetaan kanavien jatkajat ja päivitetään tieto kenttään JufoTunnus_actual
                        - Jos kanava on aktiivinen tai päättymisvuosi julkaisuvuoden jälkeen, niin JufoTunnus_actual saa arvoksi kyseisen kanavan tunnuksen
                        - Jos kanava ei ole aktiivinen tai päättymisvuosi julkaisuvuoden jälkeen, mutta sillä on jatkaja, 
                          joka on aktiivinen tai päättymisvuosi julkaisuvuoden jälkeen, niin JufoTunnus_actual saa arvoksi jatkavan kanavan tunnuksen (päättely ketjutetaan x kertaa)
                        - Jos kanava on inaktiivinen ja päättymisvuosi ennen julkaisuvuotta eikä jatkajaa ole, niin JufoTunnus_actual saa arvoksi null
                    - Poistetaan jufot_temp-taulusta ne julkaisut, joiden JufoTunnus_actual on null
                    - Varsinaisten tunnistusten jälkeen katsotaan VirtaAdditions taulusta, löytyykö tunnistamatta jääneille julkaisuille kanava

                Tunnistusten jälkeen 
                    - Haetaan kaikille kanaville jufo-luokka
                    - Päivitetään poikkeustapausten jufo-luokat
                    - Asetetaan kanavat suuruusjärjestykseen niin että korkeimman luokan omaava kanava (niistä ensimmäisenä tunnistettu) rankataan ykköseksi
                      Tämän jälkeen kullakin julkaisulla on yksi korkeimmalle rankattu julkaisukanava
            */


            // tietokantaoperaatiot.uudelleenjarjesta_indeksit("julkaisut_mds.dbo.Julkaisukanavatietokanta");


            tietokantaoperaatiot.Tyhjenna_taulu(taulu_jufot_temp);

            Action<string, string>[] metodit = new Action<string, string>[5] {
                tietokantaoperaatiot.Tunnista_konferenssi,
                tietokantaoperaatiot.Tunnista_ISSN,
                tietokantaoperaatiot.Tunnista_ISBN,
                tietokantaoperaatiot.Tunnista_ISBN_juuri,
                tietokantaoperaatiot.Hae_virta_additions
            };

            foreach (Action<string, string> metodi in metodit)
            {
                metodi.Invoke(taulu_julkaisut_temp, taulu_jufot_temp);
                tietokantaoperaatiot.Hae_kanavan_jatkajat(taulu_jufot_temp);
                tietokantaoperaatiot.Poista_Inaktiiviset(taulu_jufot_temp);
            };


            tietokantaoperaatiot.Hae_jufo_tasot(taulu_jufot_temp);
            tietokantaoperaatiot.Paivita_poikkeukset();

            tietokantaoperaatiot.Rankkaa_jufo_kanavat(taulu_jufot_temp);



            // ---------------------------------------------------------------------------------------------------------------


            //// Vaihe 3


            // Alkuperäiset jufo-tiedot talteen
            tietokantaoperaatiot.Kirjoita_alkuperaiset_jufot_tmp_tauluun();

            // Tunnistettujen tietojen päivitys alkuperäiseen tauluun
            // Tehdään ensin julkaisut_temp-tauluun, josta päättely-tieto haetaan tarkistuslokia varten

            // Tunnistetut tiedot julkaisut_temp-tauluun
            tietokantaoperaatiot.Paivita_jufot_julkaisut_temp_tauluun(taulu_julkaisut_temp, taulu_jufot_temp);

            // Tunnistetut tiedot sa-tauluun
            tietokantaoperaatiot.Paivita_jufot_sa_tauluun(taulu_julkaisut_temp);



            // ---------------------------------------------------------------------------------------------------------------


            //loppu

            //Console.WriteLine("Loppu");
            //Console.ReadLine();


        }

    }

}

