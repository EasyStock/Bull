# from sklearn.linear_model import LinearRegression

from keras.datasets import mnist
# lr = LinearRegression()
# lr.fit()
# lr.predict
# from tensorflow_datasets.datasets.binarized_mnist
# import tensorflow.keras.datasets.mnist as mnist


# # tf.keras.datasets.mnist.load_data


import requests
import tensorflow as tf


def prePorcess(x,y):
    x = tf.cast(x,dtype=tf.float32) / 255
    y = tf.cast(y,dtype=tf.int32)
    return (x,y)
if __name__ == '__main__':
    print(tf.__version__) 
    (x_train, y_train), (x_test, y_test) = mnist.load_data()
    print(x_train.shape,y_train.shape)
    print(x_test.shape,y_test.shape)
     
    # import matplotlib.pyplot as plt 
    # plt.imshow(x_train[0],cmap='gray')
    # plt.show()

    train_db = tf.data.Dataset.from_tensor_slices((x_train,y_train)).batch(32)
    train_db = train_db.map(prePorcess)
    
    test_db = tf.data.Dataset.from_tensor_slices((x_test,y_test)).batch(32)
    test_db = test_db.map(prePorcess)
    # for v in train_db:
    #     print(v[0],v[1])
    #     break
    #[1,28,28] ==> x [1,784] @ w[784,10] = [1,10 ] 
    random_normal = tf.initializers.RandomNormal()
    # L1: [None,784] @ [784,512] + [512] === > [None, 512]
    # L2: [None,512] @ [512,128] + [128] === > [None, 128]
    # L2: [None,128] @ [128,10] + [10] === > [None, 10]
    w1 = tf.Variable(initial_value=random_normal(shape=[784,512]))
    b1 = tf.Variable(initial_value=random_normal(shape=[512]))

    w2 = tf.Variable(initial_value=random_normal(shape=[512,128]))
    b2 = tf.Variable(initial_value=random_normal(shape=[128]))

    w3 = tf.Variable(initial_value=random_normal(shape=[128,10]))
    b3 = tf.Variable(initial_value=random_normal(shape=[10]))

    for index ,(x, y) in enumerate(train_db):
        #print(x.shape,y)
        x = tf.reshape(x,shape=[-1,28*28] )
        with tf.GradientTape() as taps:

            #L1 = tf.nn.relu(x @ w1 + b1)
            L1 = (x @ w1 + b1)
            L2 = (L1 @ w2 + b2)
            #y_pred = tf.nn.relu(L2 @ w3 + b3)
            #y_pred = x @ weight + bias  # 此处是预测值，而非概率
            #print(tf.argsort(y_pred))
            y_pred = tf.nn.softmax(L2 @ w3 + b3) 
            y_true = tf.one_hot(y,depth=10)
            # print(y_pred)
            # print(y_true)
            #交叉信息墒 
            loss = tf.reduce_sum(-(y_true*tf.math.log(y_pred)))
            #采用梯度下降，降低损失
            grad = taps.gradient(target=loss,sources=[w1,b1,w2,b2,w3,b3])
            #print(type(grad), grad[0],grad[1])
            lr = 0.0001 # 学习率
            w1.assign_sub(grad[0]*lr)
            b1.assign_sub(grad[1]*lr)
            w2.assign_sub(grad[2]*lr) 
            b2.assign_sub(grad[3]*lr)
            w3.assign_sub(grad[4]*lr)
            b3.assign_sub(grad[5]*lr)
            if index % 50 ==0:
                print(f'第{index}次的损失值为:{loss}')

 