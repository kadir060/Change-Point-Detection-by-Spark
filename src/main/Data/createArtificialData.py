import matplotlib.pyplot as plt
import ruptures as rpt
import numpy

def createData(n_samples: int, dim: int, sigma: int, n_bkps: int, file_name: str):
    # generate signal
    signal, bkps = rpt.pw_constant(n_samples, dim, n_bkps, noise_std=sigma)
    # # detection
    algo = rpt.Pelt(model="rbf").fit(signal)
    result = algo.predict(pen=10)
    numpy.savetxt(file_name + "_points.txt",signal)
    numpy.savetxt(file_name + "_cp.txt", bkps)
    # # display
    rpt.display(signal, bkps)
    plt.savefig(file_name + ".png")
    plt.show()

if __name__ == "__main__":
    file_name = ""
    n_samples, dim, sigma = 1000, 1, 1
    n_bkps = 4  # number of breakpoints
    file_name += str(n_samples )+ "_" + str(sigma) + "_" + str(n_bkps)
    createData(n_samples, dim, sigma, n_bkps,file_name)

