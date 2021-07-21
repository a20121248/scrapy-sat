#Funciones
import pandas as pd
import numpy as np
import io
import cv2
import re
import gc
from PIL import Image
from pytesseract import image_to_string
from urllib.request import *

def descifrarimagen(img,ancho,alto):
    imcv = np.asarray(img)
    imcv = cv2.cvtColor(imcv,cv2.COLOR_RGB2GRAY)

    first = lambda x: 255 if x > 200 or x == 0 else x
    vectorized_first = np.vectorize(first)
    imcv = vectorized_first(imcv)

    second = lambda x: 0 if x < 255 else 255
    vectorized_second = np.vectorize(second)
    imcv = vectorized_second(imcv)

    lista_de_x=[]
    for x in range(ancho):
        suma=0
        for y in range(alto):
            if (imcv[y,x]!=255):
                suma=suma+1
        if(suma<3):
            lista_de_x.append(x)

    for i in lista_de_x:
        for y in range(alto):
            imcv[y,i] = 255

    third = lambda x: 255 if x == 0 else 0
    vectorized_third = np.vectorize(third)
    imcv = vectorized_third(imcv)

    x = np.zeros((alto,ancho,3))

    x[:,:,0] = imcv
    x[:,:,1] = imcv
    x[:,:,2] = imcv

    kernel = np.ones((2,2),np.uint8)
    erosion = cv2.erode(x,kernel,iterations = 2)
    dilation = cv2.dilate(erosion,kernel,iterations = 2)

    gray = dilation[:,:,0]

    fourth = lambda x: 0 if x > 200 else 255
    vectorized_fourth = np.vectorize(fourth)
    white = vectorized_fourth(gray)
    pil_im = Image.fromarray(white.astype('uint8'))
    text = image_to_string(pil_im,config='--psm 7 --oem 3')
    listapalabra = re.findall('[a-zA-Z0-9]+',text)
    palabra = ''

    for letras in listapalabra :
        palabra = palabra + letras.upper()
    return palabra