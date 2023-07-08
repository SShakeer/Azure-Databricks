{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "2R97ADiFQ0Qq"
   },
   "source": [
    "# Strings"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "eVUSlgdsQ0Qs"
   },
   "source": [
    "    A string is a sequence of characters.\n",
    "\n",
    "    Computers do not deal with characters, they deal with numbers (binary). Even though you may see characters on your screen, internally it is stored and manipulated as a combination of 0's and 1's.\n",
    "\n",
    "    This conversion of character to a number is called encoding, and the reverse process is decoding. ASCII and Unicode are some of the popular encoding used.\n",
    "\n",
    "    In Python, string is a sequence of Unicode character."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "G_fSxHayQ0Qu"
   },
   "source": [
    "For more details about unicode \n",
    "\n",
    "https://docs.python.org/3.3/howto/unicode.html"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "d2ZdcRphQ0Qv"
   },
   "source": [
    "# How to create a string?"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "kVww6WnaQ0Qw"
   },
   "source": [
    "Strings can be created by enclosing characters inside a single quote or double quotes. \n",
    "\n",
    "Even triple quotes can be used in Python but generally used to represent multiline strings and docstrings.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "8OM3IOK2Q0Qy",
    "outputId": "01582439-98f6-42aa-814d-9b6fc38ebafd"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Hello\n",
      "Hello\n",
      "Hello\n"
     ]
    }
   ],
   "source": [
    "myString = 'Hello'\n",
    "\n",
    "print(myString)\n",
    "\n",
    "\n",
    "myString = \"Hello\"\n",
    "print(myString)\n",
    "\n",
    "\n",
    "myString = '''Hello'''\n",
    "print(myString)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "8yQfoUnGQ0Q4"
   },
   "source": [
    "# How to access characters in a string?"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "lj884ZrtQ0Q6"
   },
   "source": [
    "We can access individual characters using indexing and a range of characters using slicing.\n",
    "\n",
    "Index starts from 0. \n",
    "\n",
    "Trying to access a character out of index range will raise an IndexError. \n",
    "\n",
    "The index must be an integer. We can't use float or other types, this will result into TypeError.\n",
    "\n",
    "Python allows negative indexing for its sequences."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "dECRrHU9Q0Q6",
    "outputId": "53102267-40e2-4cfd-aec2-e5a13050778b"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "H\n",
      "o\n",
      "llo\n"
     ]
    }
   ],
   "source": [
    "myString = \"Hello\"\n",
    "\n",
    "#print first Character\n",
    "print(myString[0])\n",
    "\n",
    "#print last character using negative indexing\n",
    "print(myString[-1])\n",
    "\n",
    "#slicing 2nd to 5th character\n",
    "print(myString[2:5])"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "rYjjmAwVQ0RA"
   },
   "source": [
    "If we try to access index out of the range or use decimal number, we will get errors."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "sDdAkqPOQ0RB",
    "outputId": "62af85cf-1cd5-4cdb-9d70-85c9e3a310ef"
   },
   "outputs": [
    {
     "ename": "IndexError",
     "evalue": "string index out of range",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mIndexError\u001b[0m                                Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-2-78353fed94bc>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m()\u001b[0m\n\u001b[0;32m----> 1\u001b[0;31m \u001b[0mprint\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mmyString\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;36m15\u001b[0m\u001b[0;34m]\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
      "\u001b[0;31mIndexError\u001b[0m: string index out of range"
     ]
    }
   ],
   "source": [
    "print(myString[15])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "df79qTM1Q0RF",
    "outputId": "fa826bc9-01bf-47d9-9aef-8a2d283e49ae"
   },
   "outputs": [
    {
     "ename": "TypeError",
     "evalue": "string indices must be integers",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mTypeError\u001b[0m                                 Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-3-d32f87fd1591>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m()\u001b[0m\n\u001b[0;32m----> 1\u001b[0;31m \u001b[0mprint\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mmyString\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;36m1.5\u001b[0m\u001b[0;34m]\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
      "\u001b[0;31mTypeError\u001b[0m: string indices must be integers"
     ]
    }
   ],
   "source": [
    "print(myString[1.5])"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "l24m79F7Q0RI"
   },
   "source": [
    "# How to change or delete a string ?"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "82PqjvORQ0RK"
   },
   "source": [
    "Strings are immutable. This means that elements of a string cannot be changed once it has been assigned. \n",
    "\n",
    "We can simply reassign different strings to the same name."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "iV3SYntmQ0RL",
    "outputId": "ad24277c-3aee-442d-8e61-eab288303ec8"
   },
   "outputs": [
    {
     "ename": "TypeError",
     "evalue": "'str' object does not support item assignment",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mTypeError\u001b[0m                                 Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-6-d63a28c2a378>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m()\u001b[0m\n\u001b[1;32m      1\u001b[0m \u001b[0mmyString\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0;34m\"Hello\"\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m----> 2\u001b[0;31m \u001b[0mmyString\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;36m4\u001b[0m\u001b[0;34m]\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0;34m's'\u001b[0m \u001b[0;31m# strings are immutable\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
      "\u001b[0;31mTypeError\u001b[0m: 'str' object does not support item assignment"
     ]
    }
   ],
   "source": [
    "myString = \"Hello\"\n",
    "myString[4] = 's' # strings are immutable"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "aMWgpS3pQ0RP"
   },
   "source": [
    "We cannot delete or remove characters from a string. But deleting the string entirely is possible using the keyword       del."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "k43jqNSwQ0RQ"
   },
   "outputs": [],
   "source": [
    "del myString # delete complete string"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "3gpZMAFtQ0RT",
    "outputId": "1d4f5a0d-45e9-4a40-dab8-2419a46cd293"
   },
   "outputs": [
    {
     "ename": "NameError",
     "evalue": "name 'myString' is not defined",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mNameError\u001b[0m                                 Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-8-60c083ddb862>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m()\u001b[0m\n\u001b[0;32m----> 1\u001b[0;31m \u001b[0mprint\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mmyString\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
      "\u001b[0;31mNameError\u001b[0m: name 'myString' is not defined"
     ]
    }
   ],
   "source": [
    "print(myString)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "_I6JAqk7Q0RX"
   },
   "source": [
    "# String Operations"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "AdXL8ZfrQ0RY"
   },
   "source": [
    "# Concatenation"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "UEqPy2mBQ0Ra"
   },
   "source": [
    "Joining of two or more strings into a single one is called concatenation.\n",
    "\n",
    "The + operator does this in Python. Simply writing two string literals together also concatenates them.\n",
    "\n",
    "The * operator can be used to repeat the string for a given number of times."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "I02mfOxIQ0Rb",
    "outputId": "4f2a2f69-d794-494e-c911-ac9bbcf7905d"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Hello Satish\n",
      "Hello Hello Hello \n"
     ]
    }
   ],
   "source": [
    "s1 = \"Hello \"\n",
    "s2 = \"Satish\"\n",
    "\n",
    "#concatenation of 2 strings\n",
    "print(s1 + s2)\n",
    "\n",
    "#repeat string n times\n",
    "print(s1 * 3)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "JUHt8GFjQ0Rf"
   },
   "source": [
    "# Iterating Through String"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "jDRQiFQVQ0Rg",
    "outputId": "f1e1c6a6-612b-41e1-db98-bbb5313958d2"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "2  letters found\n"
     ]
    }
   ],
   "source": [
    "count = 0\n",
    "for l in \"Hello World\":\n",
    "    if l == 'o':\n",
    "        count += 1\n",
    "print(count, ' letters found')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "HJ_EXB0sQ0Rk"
   },
   "source": [
    "# String Membership Test"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "YCOI93eMQ0Rm",
    "outputId": "7f106a6e-9e7e-4583-a23c-7cfc5483d294"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "True\n"
     ]
    }
   ],
   "source": [
    "print('l' in 'Hello World') #in operator to test membership"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "v-Gc3e9pQ0Ru",
    "outputId": "704345a3-9ebc-4554-a983-e8a76a3b8024"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "True\n"
     ]
    }
   ],
   "source": [
    "print('or' in 'Hello World')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "kWW3wSRzQ0Rz"
   },
   "source": [
    "# String Methods"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "1Us5mMCkQ0R0"
   },
   "source": [
    "    Some of the commonly used methods are lower(), upper(), join(), split(), find(), replace() etc"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "SkMrD20MQ0R3",
    "outputId": "5d60d748-6ca0-44ba-87b3-92f868e7c754"
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "'hello'"
      ]
     },
     "execution_count": 14,
     "metadata": {
      "tags": []
     },
     "output_type": "execute_result"
    }
   ],
   "source": [
    "\"Hello\".lower()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "nz3r0uJiQ0R7",
    "outputId": "0050d3bf-c665-4b21-8208-6b60c1c25d2e"
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "'HELLO'"
      ]
     },
     "execution_count": 15,
     "metadata": {
      "tags": []
     },
     "output_type": "execute_result"
    }
   ],
   "source": [
    "\"Hello\".upper()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "wY5zUXlnQ0R_",
    "outputId": "ab0de61c-dc47-4cc4-c898-4d297eb3a0a0"
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['This', 'will', 'split', 'all', 'words', 'in', 'a', 'list']"
      ]
     },
     "execution_count": 16,
     "metadata": {
      "tags": []
     },
     "output_type": "execute_result"
    }
   ],
   "source": [
    "\"This will split all words in a list\".split()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "y8DCwXIBQ0SC",
    "outputId": "b57a1fc4-7191-4071-971f-acef6617928f"
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "'This will split all words in a list'"
      ]
     },
     "execution_count": 26,
     "metadata": {
      "tags": []
     },
     "output_type": "execute_result"
    }
   ],
   "source": [
    "' '.join(['This', 'will', 'split', 'all', 'words', 'in', 'a', 'list'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "Rp2Kj-qkQ0SF",
    "outputId": "390b72be-bd82-4903-bc08-285f87faa4c0"
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "5"
      ]
     },
     "execution_count": 18,
     "metadata": {
      "tags": []
     },
     "output_type": "execute_result"
    }
   ],
   "source": [
    "\"Good Morning\".find(\"Mo\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "GWLYw7alQ0SI",
    "outputId": "e842a03d-1836-460e-c5e3-a3cf80984803"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Bad morning\n",
      "Good morning\n"
     ]
    }
   ],
   "source": [
    "s1 = \"Bad morning\"\n",
    "\n",
    "s2 = s1.replace(\"Bad\", \"Good\")\n",
    "\n",
    "print(s1)\n",
    "print(s2)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "i_H-coiDQ0SL"
   },
   "source": [
    "# Python Program to Check where a String is Palindrome or not ?"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "PhIi2cEqQ0SM",
    "outputId": "230eeef0-534f-4e65-9602-14b990cf2a51"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Given String is palindrome\n"
     ]
    }
   ],
   "source": [
    "myStr = \"Madam\"\n",
    "\n",
    "#convert entire string to either lower or upper\n",
    "myStr = myStr.lower()\n",
    "\n",
    "#reverse string\n",
    "revStr = reversed(myStr)\n",
    "\n",
    "\n",
    "#check if the string is equal to its reverse\n",
    "if list(myStr) == list(revStr):\n",
    "    print(\"Given String is palindrome\")\n",
    "else:\n",
    "    print(\"Given String is not palindrome\")\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "TLQRLF-_Q0SQ"
   },
   "source": [
    "# Python Program to Sort Words in Alphabetic Order?"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "8c75-LV9Q0SS",
    "outputId": "1c220dee-b732-426d-938d-51308151ab72"
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Alphabetic\n",
      "Order\n",
      "Program\n",
      "Sort\n",
      "in\n",
      "python\n",
      "to\n",
      "words\n"
     ]
    }
   ],
   "source": [
    "myStr = \"python Program to Sort words in Alphabetic Order\"\n",
    "\n",
    "#breakdown the string into list of words\n",
    "words = myStr.split()\n",
    "\n",
    "#sort the list\n",
    "words.sort()\n",
    "\n",
    "#print Sorted words are\n",
    "for word in words:\n",
    "    print(word)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "F3GTmmkUQ0SV"
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "colab": {},
    "colab_type": "code",
    "id": "8S483u74Q0SY"
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "colab": {
   "name": "2.6.ipynb",
   "provenance": [],
   "version": "0.3.2"
  },
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 1
}
