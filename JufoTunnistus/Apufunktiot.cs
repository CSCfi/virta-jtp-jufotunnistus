

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


        public string muokkaa_nimea(string nimi)
        {
            if (nimi == null || nimi.Equals(""))
            {
                return null;
            }

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


        public string muokkaa_DOI(string doi)
        {

            // Muokataan DOI-tunnusta, mikali se ei ole null ja pituus on yli 2. 
            // Muokkauksella halutaan, etta DOI-tunnus alkaa oikeassa muodossa eli etta ensimmainen merkki on 1 ja toinen 0.
            if (doi == null || doi.Equals(""))
            {
                return null;
            }

            if (doi.Length <= 2)
            {
                return doi;
            }

            // trimmataan aluksi doi
            string newDOI = doi.Trim();

            // poistetaan sitten alusta merkkeja siihen asti kunnes kaksi ensimmaista merkkia ovat 10
            int pituus = newDOI.Length;

            bool loopContinues = true;

            if (pituus <= 2)
            {
                loopContinues = false;
            }

            while (loopContinues)
            {

                char ekaMerkki = newDOI[0];
                char tokaMerkki = newDOI[1];

                // doi alkaa oikein, siis ekat kaksi merkkia ovat 10
                if ((ekaMerkki == '1') && (tokaMerkki == '0'))
                {
                    return newDOI;
                }

                else
                {
                    newDOI = newDOI.Substring(1);
                }

                pituus = newDOI.Length;

                if (pituus <= 2)
                {
                    loopContinues = false;
                }

            }

            return newDOI;

        }


    }

}

