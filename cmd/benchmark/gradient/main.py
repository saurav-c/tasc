import numpy as np

def main():
    gradient_descent(gradient, start, learn_rate, n_iter, tolerance)

def gradient_descrent(gradient, start, learn_rate, n_iter, tolerance):
    vector = start
    for _ in range(n_iter):
        diff = -learn_rate * gradient(vector)
        vector += addVectors(vector, diff)
    return vector

def gradient(vector):


if __name__ == '__main__':
    main()