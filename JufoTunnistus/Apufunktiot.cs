using System;
using System.Text.RegularExpressions;

namespace Jufo_Tunnistus
{
    class Apufunktiot
    {

        // nama stop charsit hypataan yli eli naita ei oteta mukaan pitkissa stringeissa (julkaisun nimi, kustantaja jne.)
        private readonly string[] stop_chars = { "!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/", ":", ";", "<", "=", ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~", "£", "¿",
                                        "®", "¬", "½", "¼", "«", "»", "©", "┐", "└", "┴", "┬", "├", "─", "┼", "┘", "┌", "¦", "¯", "´", "≡", "±", "‗", "¾", "¶", "§", "÷", "¸", "°", "¨", "·", "¹", "³", "²" };

        // nama stop wordsit hypataan yli eli naita ei oteta huomioon pitkissa stringeissa (julkaisun nimi, kustantaja jne.)
        private readonly string[] stop_words = {" i "," me "," my "," myself "," we "," our "," ours "," ourselves "," you "," your "," yours "," yourself "," yourselves "," he "," him "," his "," himself "," she "," her "," hers "," herself "," it "," its "," itself "," they "," them "," their "," theirs ",
" themselves "," what "," which "," who "," whom "," this "," that "," these "," those "," am "," is "," are "," was "," were "," be "," been "," being "," have "," has "," had "," having "," do "," does "," did "," doing "," a "," an "," the "," and "," but "," if "," or "," because "," as "," until ",
" while "," of "," at "," by "," for "," with "," about "," against "," between "," into "," through "," during "," before "," after "," above "," below "," to "," from "," up "," down "," in "," out "," on "," off "," over "," under "," again "," further "," then "," once "," here "," there ",
" when "," where "," why "," how "," all "," any "," both "," each "," few "," more "," most "," other "," some "," such "," no "," nor "," not "," only "," own "," same "," so "," than "," too "," very "," s "," t "," can "," will "," just "," don "," should "," now "};


        public string Muokkaa_nimea(string nimi)
        {

            if (string.IsNullOrWhiteSpace(nimi))
            {
                return null;
            }

            // Muutetaan nimi LowerCase:ksi ja trimmataan
            nimi = nimi.ToLower().Trim();

            // Käydään läpi stop_chars -merkit ja poistetaan merkki mikäli se löytyy nimestä
            foreach (string c in stop_chars)
            {
                nimi = nimi.Replace(c, " ");
            }

            // Käydään läpi stop_words -sanat ja poistetaan sana mikäli se löytyy nimestä
            foreach (string word in stop_words)
            {
                nimi = nimi.Replace(word, " ");
            }

            // Poistetaan tyhjät välimerkit
            nimi = Regex.Replace(nimi, @"\s+", " ").Trim();

            // Poistetaan sitten nimen alusta sanat the, a ja an
            string[] words = nimi.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (words.Length > 0 && (words[0] == "the" || words[0] == "a" || words[0] == "an"))
            {
                nimi = string.Join(" ", words, 1, words.Length - 1);
            }

            return nimi;

        }


        public string Muokkaa_DOI(string doi)
        {

            // Muokkauksella halutaan, etta DOI-tunnus alkaa oikeassa muodossa eli että ensimmäinen merkki on 1 ja toinen 0.

            // Muokataan DOI-tunnusta, mikäli se ei ole null ja pituus on yli 2. 
            if (string.IsNullOrWhiteSpace(doi))
            {
                return null;
            }

            // Trimmataan doi
            string newDOI = doi.Trim();

            if (newDOI.Length <= 2)
            {
                return newDOI;
            }

            // Poistetaan sitten alusta merkkejä siihen asti kunnes kaksi ensimmäistä merkkiä ovat 10
            while (newDOI.Length > 2 && newDOI.Substring(0, 2) != "10")
            {
                newDOI = newDOI.Substring(1);
            }

            // Palautetaan muokkaamaton DOI jos jäi jäljelle vain kaksi viimeistä numeroa
            if (newDOI.Length == 2)
            {
                return doi;
            }

            return newDOI;

        }


    }

}

